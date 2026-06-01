open Base

let src = Logs.Src.create "awskit-eio" ~doc:"AWS Eio HTTP"

module Log = (val Logs.src_log src : Logs.LOG)

type net = Net : _ Eio.Net.t -> net
type time_clock = Time_clock : _ Eio.Time.clock -> time_clock

type conn = {
  net : net;
  time_clock : time_clock;
  sw : Eio.Switch.t;
  region : Awskit.Region.t;
  credentials : Awskit.Credentials.t;
  clock : unit -> Ptime.t;
  retry_policy : Awskit.Retry.t;
  endpoint : Awskit.Endpoint.t option;
  max_response_body_bytes : int;
}

type stream_item = Chunk of string | End | Failed of string

module Stream_source = struct
  type t = {
    stream : stream_item Eio.Stream.t;
    mutable chunk : string;
    mutable offset : int;
  }

  let rec next_chunk t =
    match Eio.Stream.take t.stream with
    | End -> raise End_of_file
    | Failed message -> failwith message
    | Chunk chunk when String.is_empty chunk -> next_chunk t
    | Chunk chunk ->
        t.chunk <- chunk;
        t.offset <- 0

  let single_read t dst =
    if t.offset >= String.length t.chunk then next_chunk t;
    let len = min (Cstruct.length dst) (String.length t.chunk - t.offset) in
    Cstruct.blit_from_string t.chunk t.offset dst 0 len;
    t.offset <- t.offset + len;
    len

  let read_methods = []
end

let stream_source =
  let ops = Eio.Flow.Pi.source (module Stream_source) in
  fun stream ->
    Eio.Resource.T ({ Stream_source.stream; chunk = ""; offset = 0 }, ops)

type upload_writer = {
  stream : stream_item Eio.Stream.t;
  remaining : int64 option ref;
  mutable write_error : Awskit.Error.t option;
}

type upload_body =
  | Source of Awskit.Body.Upload.descriptor * Cohttp_eio.Body.t
  | Stream of
      Awskit.Body.Upload.descriptor
      * (upload_writer -> (unit, Awskit.Error.t) Result.t)

type download_body = { body : Cohttp_eio.Body.t; max_response_body_bytes : int }

type download_reader = {
  body : Cohttp_eio.Body.t;
  mutable chunk : string;
  mutable offset : int;
}

type upload_bridge = {
  body : Cohttp_eio.Body.t option;
  finished : (unit, Awskit.Error.t) Result.t Eio.Promise.t;
}

let default_max_response_body_bytes = 64 * 1024 * 1024
let authenticator = lazy (Ca_certs.authenticator ())

let tls_config =
  lazy
    (match Lazy.force authenticator with
    | Error (`Msg msg) ->
        invalid_arg
          (Fmt.str
             "Awskit_eio.create: failed to create system X509 authenticator: %s"
             msg)
    | Ok authenticator -> (
        match Tls.Config.client ~authenticator () with
        | Error (`Msg msg) ->
            invalid_arg
              (Fmt.str "Awskit_eio.create: failed to create TLS config: %s" msg)
        | Ok config -> config))

let ensure_tls_runtime () = Mirage_crypto_rng_unix.use_default ()

let https_connector uri raw =
  ensure_tls_runtime ();
  let host =
    Uri.host uri
    |> Option.map ~f:(fun x -> Domain_name.(host_exn (of_string_exn x)))
  in
  Tls_eio.client_of_flow ?host (Lazy.force tls_config) raw

let create ~env ~sw ~region ~credentials ?(clock = Ptime_clock.now)
    ?(retry_policy = Awskit.Retry.default) ?endpoint
    ?(max_response_body_bytes = default_max_response_body_bytes) () =
  if max_response_body_bytes <= 0 then
    invalid_arg "Awskit_eio.create: max_response_body_bytes must be positive";
  let net = Net (env :> < net : _ Eio.Net.t ; .. >)#net in
  let time_clock =
    Time_clock (env :> < clock : _ Eio.Time.clock ; .. >)#clock
  in
  {
    net;
    time_clock;
    sw;
    region;
    credentials;
    clock;
    retry_policy;
    endpoint;
    max_response_body_bytes;
  }

let to_cohttp_meth = function
  | `GET -> `GET
  | `PUT -> `PUT
  | `POST -> `POST
  | `DELETE -> `DELETE
  | `HEAD -> `HEAD
  | `PATCH -> `PATCH

let to_aws_response http_response =
  Awskit.Response.create_exn
    ~status:(Http.Response.status http_response |> Http.Status.to_int)
    ~headers:(Http.Response.headers http_response |> Http.Header.to_list)
    ()

let make_uri (request : Awskit.Request.t) =
  let target = request.target in
  let scheme_str = Awskit.Endpoint.Scheme.to_string target.scheme in
  let host_port =
    match target.port with
    | Some port -> Fmt.str "%s:%d" target.host port
    | None -> target.host
  in
  Uri.of_string
    (Fmt.str "%s://%s%s" scheme_str host_port
       (Awskit.Request.Target.path_and_query target))

let descriptor_for_string body =
  {
    Awskit.Body.Upload.content_length = Some (String.length body |> Int64.of_int);
    payload_hash = Awskit.Body.Payload_hash.sha256_of_string body;
    replayable = true;
  }

let empty_body = Source (descriptor_for_string "", Cohttp_eio.Body.of_string "")

let string_body body =
  Source (descriptor_for_string body, Cohttp_eio.Body.of_string body)

let bytes_body body =
  let body = Bytes.to_string body in
  string_body body

let stream_body descriptor ~write = Stream (descriptor, write)

let upload_descriptor = function
  | Source (descriptor, _) -> descriptor
  | Stream (descriptor, _) -> descriptor

let body_error message = Awskit.Error.body message

let drain_limit_error max_response_body_bytes =
  Awskit.Error.body
    ~limit:(Int64.of_int max_response_body_bytes)
    "response body exceeded max_response_body_bytes"

let writer_for descriptor stream =
  {
    stream;
    remaining = ref descriptor.Awskit.Body.Upload.content_length;
    write_error = None;
  }

let check_write_length writer string =
  match !(writer.remaining) with
  | None -> Ok ()
  | Some remaining ->
      let length = Int64.of_int (String.length string) in
      if Stdlib.Int64.compare length remaining > 0 then
        Error (body_error "upload body exceeded declared content_length")
      else (
        writer.remaining := Some (Stdlib.Int64.sub remaining length);
        Ok ())

let check_finished_length writer =
  match writer.write_error with
  | Some error -> Error error
  | None -> (
      match !(writer.remaining) with
      | None | Some 0L -> Ok ()
      | Some _ ->
          Error (body_error "upload body ended before declared content_length"))

let write_string writer string =
  match writer.write_error with
  | Some error -> Error error
  | None -> (
      match check_write_length writer string with
      | Error error ->
          writer.write_error <- Some error;
          Error error
      | Ok () ->
          Eio.Stream.add writer.stream (Chunk string);
          Ok ())

let body_to_cohttp ~sw = function
  | Source (_, body) ->
      { body = Some body; finished = Eio.Promise.create_resolved (Ok ()) }
  | Stream (descriptor, write) ->
      let stream = Eio.Stream.create 16 in
      let writer = writer_for descriptor stream in
      let upload_finished, wake_upload_finished = Eio.Promise.create () in
      let finish result =
        ignore (Eio.Promise.try_resolve wake_upload_finished result : bool)
      in
      Eio.Fiber.fork ~sw (fun () ->
          match write writer with
          | Ok () ->
              let result = check_finished_length writer in
              Eio.Stream.add stream End;
              finish result
          | Error error ->
              Log.warn (fun m ->
                  m "upload stream failed: %s"
                    (Awskit.Error.to_string_hum error));
              finish (Error error);
              Eio.Stream.add stream (Failed (Awskit.Error.to_string_hum error))
          | exception (Eio.Cancel.Cancelled _ as exn) -> raise exn
          | exception exn ->
              let error = body_error (Exn.to_string exn) in
              Log.warn (fun m ->
                  m "upload stream raised: %s"
                    (Awskit.Error.to_string_hum error));
              finish (Error error);
              Eio.Stream.add stream (Failed (Awskit.Error.to_string_hum error)));
      { body = Some (stream_source stream); finished = upload_finished }

let do_with_response (conn : conn) (request : Awskit.Request.t) upload_body ~f =
  let (Net net) = conn.net in
  let https =
    match request.target.scheme with
    | `Http -> None
    | `Https -> Some https_connector
  in
  let client = Cohttp_eio.Client.make ~https net in
  let headers = Http.Header.of_list request.headers in
  let uri = make_uri request in
  let meth = to_cohttp_meth request.method_ in
  let make_download_body body =
    { body; max_response_body_bytes = conn.max_response_body_bytes }
  in
  let successful_status status = status >= 200 && status < 300 in
  let early_result = ref None in
  let exception Early_response in
  let exception Callback_raised of exn in
  let call_f response body =
    match f response body with
    | result -> result
    | exception exn -> raise (Callback_raised exn)
  in
  let run_call ~call_sw =
    let bridge = body_to_cohttp ~sw:call_sw upload_body in
    try
      let response, body =
        Cohttp_eio.Client.call client ~sw:call_sw ~headers ?body:bridge.body
          meth uri
      in
      let status = Http.Response.status response |> Http.Status.to_int in
      let upload_result =
        if successful_status status then Eio.Promise.await bridge.finished
        else
          match Eio.Promise.peek bridge.finished with
          | Some result -> result
          | None ->
              early_result :=
                Some
                  (call_f (to_aws_response response) (make_download_body body));
              raise Early_response
      in
      match upload_result with
      | Error _ as error -> error
      | Ok () ->
          Log.debug (fun m -> m "HTTP %d" status);
          call_f (to_aws_response response) (make_download_body body)
    with
    | Early_response as exn -> raise exn
    | Callback_raised exn -> raise exn
    | Eio.Cancel.Cancelled _ as exn -> raise exn
    | exn -> (
        match Eio.Promise.peek bridge.finished with
        | Some (Error error) -> Error error
        | _ ->
            let message = Exn.to_string exn in
            Log.warn (fun m -> m "HTTP call failed: %s" message);
            Error (Awskit.Error.transport ~retryable:true message))
  in
  match upload_body with
  | Source _ -> run_call ~call_sw:conn.sw
  | Stream _ -> (
      try
        Eio.Switch.run ~name:"awskit stream upload attempt" (fun upload_sw ->
            run_call ~call_sw:upload_sw)
      with Early_response -> (
        match !early_result with
        | Some result -> result
        | None -> Error (body_error "missing early response result")))

type +'a t = 'a

let return x = x
let bind x f = f x

type connection = conn

let now c = c.clock ()
let region c = c.region
let credentials c = Ok c.credentials
let endpoint c = c.endpoint
let retry_policy c = c.retry_policy

let sleep c span =
  let (Time_clock clock) = c.time_clock in
  Eio.Time.sleep clock (Ptime.Span.to_float_s span)

let invalid_read_bounds bytes ~off ~len =
  off < 0 || len < 0 || len > Bytes.length bytes - off

let rec read_from_current reader bytes ~off ~len =
  if len = 0 then Ok 0
  else if reader.offset < String.length reader.chunk then begin
    let available = String.length reader.chunk - reader.offset in
    let copied = min available len in
    Stdlib.String.blit reader.chunk reader.offset bytes off copied;
    reader.offset <- reader.offset + copied;
    Ok copied
  end
  else
    let buffer = Cstruct.create 0x8000 in
    let read = Eio.Flow.single_read reader.body buffer in
    reader.chunk <- Cstruct.to_string (Cstruct.sub buffer 0 read);
    reader.offset <- 0;
    read_from_current reader bytes ~off ~len

let read reader bytes ~off ~len =
  if invalid_read_bounds bytes ~off ~len then
    Error (Awskit.Error.body "invalid read bounds")
  else
    try read_from_current reader bytes ~off ~len with
    | End_of_file -> Ok 0
    | Eio.Cancel.Cancelled _ as exn -> raise exn
    | exn -> Error (Awskit.Error.body (Exn.to_string exn))

let rec discard_reader reader ~remaining ~max_response_body_bytes =
  let buffer = Bytes.create 8192 in
  let len = if remaining <= 0 then 1 else min (Bytes.length buffer) remaining in
  match read reader buffer ~off:0 ~len with
  | Error _ as error -> error
  | Ok 0 -> Ok ()
  | Ok n ->
      if n > remaining then Error (drain_limit_error max_response_body_bytes)
      else
        discard_reader reader ~remaining:(remaining - n)
          ~max_response_body_bytes

let discard_download_reader (reader : download_reader) (body : download_body) =
  discard_reader reader ~remaining:body.max_response_body_bytes
    ~max_response_body_bytes:body.max_response_body_bytes

let with_download_body (body : download_body) ~consume =
  let reader = { body = body.body; chunk = ""; offset = 0 } in
  match consume reader with
  | Ok _ as result -> (
      match discard_download_reader reader body with
      | Ok () -> result
      | Error _ as error -> error)
  | Error _ as error -> (
      match discard_download_reader reader body with
      | Ok () -> error
      | Error _ as drain_error -> drain_error)

let discard_download_body (body : download_body) =
  discard_download_reader { body = body.body; chunk = ""; offset = 0 } body

let with_response = do_with_response
