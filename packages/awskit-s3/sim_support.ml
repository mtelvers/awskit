open Core

module Clock = struct
  type t = { mutable now : Ptime.t }

  let create ?(now = Ptime.epoch) () = { now }
  let now t = t.now

  let advance t span =
    t.now <- Option.value ~default:t.now (Ptime.add_span t.now span)

  let advance_ms t ms = advance t (Ptime.Span.of_int_s (ms / 1000))
end

type config = { max_list_keys : int }

let default_config = { max_list_keys = 1000 }

type stored_object = {
  mutable body : string;
  mutable etag : Object.Etag.t;
  mutable version_id : Object.Version_id.t option;
  mutable content_type : string option;
  mutable metadata : Metadata.t;
  mutable storage_class : Storage_class.t option;
  mutable tags : Tag.t list;
  mutable checksum : Object.Checksum.response option;
  mutable last_modified : Ptime.t;
}

type stored_delete_marker = {
  version_id : Object.Version_id.t;
  last_modified : Ptime.t;
}

type stored_version =
  | Stored_object of stored_object
  | Stored_delete_marker of stored_delete_marker

type stored_part = {
  part_number : int;
  body : string;
  etag : Object.Etag.t;
  checksum : Object.Checksum.response option;
  last_modified : Ptime.t;
}

type multipart_upload = {
  upload : Multipart.Upload.t;
  content_type : string option;
  metadata : Metadata.t;
  storage_class : Storage_class.t option;
  tags : Tag.t list;
  checksum_request : Object.Checksum.request option;
  parts : (int, stored_part) Hashtbl.t;
  created_at : Ptime.t;
}

type bucket_state = {
  created_at : Ptime.t;
  objects : (string, stored_version) Hashtbl.t;
  versions : (string, stored_version list) Hashtbl.t;
  multipart_uploads : (string, multipart_upload) Hashtbl.t;
  mutable policy : Policy.t option;
  mutable bucket_tags : Tag.t list;
  mutable versioning : Bucket.Versioning.Status.t option;
  mutable encryption : Bucket.Encryption.config option;
  mutable cors : Bucket.Cors.config option;
  mutable website : Bucket.Website.config option;
  mutable public_access_block : Bucket.Public_access_block.config option;
  mutable ownership_controls : Bucket.Ownership_controls.config option;
  mutable request_payment : Bucket.Request_payment.Payer.t option;
  mutable accelerate : Bucket.Accelerate.Status.t option;
  mutable logging : Bucket.Logging.config;
}

type op_record = {
  op :
    [ `Put
    | `Get
    | `Head
    | `Delete
    | `List
    | `List_versions
    | `Copy
    | `Delete_many
    | `Multipart_create
    | `Multipart_upload_part
    | `Multipart_complete
    | `Multipart_abort
    | `Multipart_list_parts ];
  bucket : string;
  key : string option;
  timestamp : Ptime.t;
  faulted : bool;
}

type store = {
  config : config;
  clock : Clock.t;
  buckets : (string, bucket_state) Hashtbl.t;
  mutable history : op_record list;
  mutable next_upload_id : int;
  mutable next_version_id : int;
}

let create_store ?(config = default_config) ~clock () =
  {
    config;
    clock;
    buckets = Hashtbl.create 17;
    history = [];
    next_upload_id = 1;
    next_version_id = 1;
  }

type fault = Slow_down | Internal_error | Connection_reset | Response_lost
type buggify = { random : Random.State.t; prob : float }

type t = {
  store : store;
  credentials : Awskit.Credentials.t;
  mutable faults : fault list;
  mutable buggify : buggify option;
}

let connect store ~credentials =
  { store; credentials; faults = []; buggify = None }

let store t = t.store
let now t = Clock.now t.store.clock

let service ?message ?(headers = []) ~status ~code () =
  Awskit.Error.service
    {
      status;
      code = Some code;
      message;
      request_id = None;
      host_id = None;
      headers;
      body = None;
    }

let no_such_bucket () = service ~status:404 ~code:"NoSuchBucket" ()
let no_such_key () = service ~status:404 ~code:"NoSuchKey" ()
let no_such_upload () = service ~status:404 ~code:"NoSuchUpload" ()

let method_not_allowed ?headers () =
  service ?headers ~status:405 ~code:"MethodNotAllowed" ()

let precondition_failed () = service ~status:412 ~code:"PreconditionFailed" ()
let not_modified () = service ~status:304 ~code:"NotModified" ()
let response ?headers status = Awskit.Response.create_exn ~status ?headers ()

let record ?(faulted = false) t op bucket key =
  t.store.history <-
    { op; bucket; key; timestamp = now t; faulted } :: t.store.history

let fault_error = function
  | Slow_down -> service ~status:503 ~code:"SlowDown" ()
  | Internal_error -> service ~status:500 ~code:"InternalError" ()
  | Connection_reset ->
      Awskit.Error.transport ~retryable:true "simulated connection reset"
  | Response_lost -> Awskit.Error.body "simulated response body loss"

let take_fault t =
  match t.faults with
  | fault :: rest ->
      t.faults <- rest;
      Some fault
  | [] -> (
      match t.buggify with
      | None -> None
      | Some buggify ->
          if Random.State.float buggify.random 1.0 < buggify.prob then
            Some Internal_error
          else None)

let operation_fault t op bucket key =
  match take_fault t with
  | None ->
      record t op bucket key;
      None
  | Some fault ->
      record ~faulted:true t op bucket key;
      Some (fault_error fault)

let bucket_state store bucket = Hashtbl.find_opt store.buckets bucket

let require_bucket t bucket =
  match bucket_state t.store bucket with
  | Some state -> Ok state
  | None -> Error (no_such_bucket ())

let require_object t bucket key =
  let* bucket = require_bucket t bucket in
  match Hashtbl.find_opt bucket.objects key with
  | Some (Stored_object object_) -> Ok object_
  | None -> Error (no_such_key ())
  | Some (Stored_delete_marker _) -> Error (no_such_key ())

let versioning_enabled (bucket : bucket_state) =
  match bucket.versioning with
  | Some Bucket.Versioning.Status.Enabled -> true
  | Some Suspended | None -> false

let versioning_suspended (bucket : bucket_state) =
  match bucket.versioning with
  | Some Bucket.Versioning.Status.Suspended -> true
  | Some Enabled | None -> false

let versioning_keeps_history bucket =
  versioning_enabled bucket || versioning_suspended bucket

let next_version_id t =
  let id = t.store.next_version_id in
  t.store.next_version_id <- id + 1;
  Object.Version_id.of_string_exn (Fmt.str "sim-version-%d" id)

let null_version_id = Object.Version_id.of_string_exn "null"

let version_id_of_version = function
  | Stored_object obj -> obj.version_id
  | Stored_delete_marker marker -> Some marker.version_id

let prepend_version bucket key version =
  let versions =
    match Hashtbl.find_opt bucket.versions key with
    | None -> []
    | Some versions -> versions
  in
  Hashtbl.replace bucket.versions key (version :: versions)

let replace_null_version bucket key version =
  let versions =
    match Hashtbl.find_opt bucket.versions key with
    | None -> []
    | Some versions ->
        List.filter
          (fun existing ->
            match version_id_of_version existing with
            | Some version_id ->
                not (Object.Version_id.equal version_id null_version_id)
            | None -> true)
          versions
  in
  Hashtbl.replace bucket.versions key (version :: versions)

let store_object t bucket key (obj : stored_object) =
  let obj =
    if versioning_enabled bucket then
      { obj with version_id = Some (next_version_id t) }
    else if versioning_suspended bucket then
      { obj with version_id = Some null_version_id }
    else obj
  in
  let version = Stored_object obj in
  Hashtbl.replace bucket.objects key version;
  if versioning_suspended bucket then replace_null_version bucket key version
  else if Option.is_some obj.version_id then prepend_version bucket key version;
  obj

let find_version bucket key version_id =
  match Hashtbl.find_opt bucket.versions key with
  | None -> None
  | Some versions ->
      List.find_opt
        (fun version ->
          match version_id_of_version version with
          | Some candidate -> Object.Version_id.equal candidate version_id
          | None -> false)
        versions

let current_or_version bucket key version_id =
  match version_id with
  | None -> Hashtbl.find_opt bucket.objects key
  | Some version_id -> find_version bucket key version_id

let current_object bucket key =
  match Hashtbl.find_opt bucket.objects key with
  | Some (Stored_object obj) -> Some obj
  | None | Some (Stored_delete_marker _) -> None

let require_object_version t bucket key version_id =
  let* bucket = require_bucket t bucket in
  match current_or_version bucket key version_id with
  | Some (Stored_object object_) -> Ok object_
  | None -> Error (no_such_key ())
  | Some (Stored_delete_marker _) -> Error (no_such_key ())

let delete_version bucket key version_id =
  match Hashtbl.find_opt bucket.versions key with
  | None -> None
  | Some versions -> (
      let deleted = ref None in
      let remaining =
        List.filter
          (fun version ->
            match version_id_of_version version with
            | Some candidate when Object.Version_id.equal candidate version_id
              ->
                deleted := Some version;
                false
            | _ -> true)
          versions
      in
      match !deleted with
      | None -> None
      | Some deleted_version ->
          (match remaining with
          | [] ->
              Hashtbl.remove bucket.versions key;
              Hashtbl.remove bucket.objects key
          | latest :: _ ->
              Hashtbl.replace bucket.versions key remaining;
              Hashtbl.replace bucket.objects key latest);
          Some deleted_version)

let store_delete_marker t bucket key =
  let marker =
    {
      version_id =
        (if versioning_suspended bucket then null_version_id
         else next_version_id t);
      last_modified = now t;
    }
  in
  let version = Stored_delete_marker marker in
  Hashtbl.replace bucket.objects key version;
  if versioning_suspended bucket then replace_null_version bucket key version
  else prepend_version bucket key version;
  marker

let version_headers = function
  | None -> []
  | Some version_id ->
      [ ("x-amz-version-id", Object.Version_id.to_string version_id) ]

let copy_source_version_headers = function
  | None -> []
  | Some version_id ->
      [
        ("x-amz-copy-source-version-id", Object.Version_id.to_string version_id);
      ]

let delete_marker_headers = function
  | None | Some false -> []
  | Some true -> [ ("x-amz-delete-marker", "true") ]

let delete_marker_error ~current marker =
  let headers =
    version_headers (Some marker.version_id) @ delete_marker_headers (Some true)
  in
  if current then service ~headers ~status:404 ~code:"NoSuchKey" ()
  else method_not_allowed ~headers ()

let object_size (obj : stored_object) = Int64.of_int (String.length obj.body)

let etag_condition_matches (obj : stored_object) = function
  | Object.Etag_condition.Any -> true
  | Etag etag -> Object.Etag.equal obj.etag etag

let etag_condition_matches_opt obj condition =
  match obj with
  | None -> false
  | Some obj -> etag_condition_matches obj condition

let ensure_write_preconditions obj (p : Object.Preconditions.Write.t) =
  match p.if_match with
  | Some condition when not (etag_condition_matches_opt obj condition) ->
      Error (precondition_failed ())
  | _ -> (
      match (p.if_none_match, obj) with
      | Some Object.Etag_condition.Any, Some _ -> Error (precondition_failed ())
      | Some (Etag etag), Some obj when Object.Etag.equal obj.etag etag ->
          Error (precondition_failed ())
      | _ -> Ok ())

let time_leq left right = Ptime.compare left right <= 0
let time_gt left right = Ptime.compare left right > 0

let ensure_read_preconditions (obj : stored_object)
    (p : Object.Preconditions.Read.t) =
  match p.if_match with
  | Some condition when not (etag_condition_matches obj condition) ->
      Error (precondition_failed ())
  | _ -> (
      match p.if_unmodified_since with
      | Some time when time_gt obj.last_modified time ->
          Error (precondition_failed ())
      | _ -> (
          match p.if_none_match with
          | Some condition when etag_condition_matches obj condition ->
              Error (not_modified ())
          | _ -> (
              match p.if_modified_since with
              | Some time when time_leq obj.last_modified time ->
                  Error (not_modified ())
              | _ -> Ok ())))

let ensure_delete_preconditions (obj : stored_object)
    (p : Object.Preconditions.Delete.t) =
  match p.if_match with
  | Some condition when not (etag_condition_matches obj condition) ->
      Error (precondition_failed ())
  | _ -> (
      match p.if_match_last_modified_time with
      | Some time when Ptime.compare obj.last_modified time <> 0 ->
          Error (precondition_failed ())
      | _ -> (
          match p.if_match_size with
          | Some size when Int64.compare (object_size obj) size <> 0 ->
              Error (precondition_failed ())
          | _ -> Ok ()))

let delete_preconditions_are_empty (p : Object.Preconditions.Delete.t) =
  Option.is_none p.if_match
  && Option.is_none p.if_match_last_modified_time
  && Option.is_none p.if_match_size

let ensure_copy_source_preconditions (obj : stored_object)
    (p : Object.Preconditions.Copy_source.t) =
  match p.if_match with
  | Some condition when not (etag_condition_matches obj condition) ->
      Error (precondition_failed ())
  | _ -> (
      match p.if_unmodified_since with
      | Some time when time_gt obj.last_modified time ->
          Error (precondition_failed ())
      | _ -> (
          match p.if_none_match with
          | Some condition when etag_condition_matches obj condition ->
              Error (precondition_failed ())
          | _ -> (
              match p.if_modified_since with
              | Some time when time_leq obj.last_modified time ->
                  Error (precondition_failed ())
              | _ -> Ok ())))

let upload_key upload_id = Multipart.Upload_id.to_string upload_id

let require_multipart_upload t ~bucket ~key ~upload_id =
  let* bucket_state = require_bucket t bucket in
  match
    Hashtbl.find_opt bucket_state.multipart_uploads (upload_key upload_id)
  with
  | Some upload when String.equal upload.upload.key key ->
      Ok (bucket_state, upload)
  | _ -> Error (no_such_upload ())

let etag body = "\"" ^ Digestif.MD5.(digest_string body |> to_hex) ^ "\""

let checksum_of_request ~body (request : Object.Checksum.request) =
  let value =
    match request.value with
    | Some value -> Some value
    | None -> (
        match request.algorithm with
        | `SHA1 ->
            Some
              (Digestif.SHA1.(digest_string body |> to_raw_string)
              |> Base64.encode_exn)
        | `SHA256 ->
            Some
              (Digestif.SHA256.(digest_string body |> to_raw_string)
              |> Base64.encode_exn)
        | `CRC32 | `CRC32C | `CRC64NVME -> None)
  in
  Option.map
    (fun value -> { Object.Checksum.algorithm = request.algorithm; value })
    value

let checksum_for_body ~body request =
  Option.bind request (checksum_of_request ~body)

let checksum_response_headers = function
  | None -> []
  | Some (checksum : Object.Checksum.response) ->
      [ (checksum_header_name checksum.algorithm, checksum.value) ]

let next_upload_id t =
  let id = t.store.next_upload_id in
  t.store.next_upload_id <- id + 1;
  Multipart.Upload_id.of_string_exn (Fmt.str "sim-upload-%d" id)

module Runtime = struct
  type connection = t
  type 'a t = 'a

  let return x = x
  let bind x f = f x

  type upload_body = {
    descriptor : Awskit.Body.Upload.descriptor;
    body : (string, Awskit.Error.t) result;
  }

  type download_body = { body : string; read_fault : Awskit.Error.t option }

  type upload_writer = {
    buffer : Buffer.t;
    remaining : int64 option ref;
    mutable write_error : Awskit.Error.t option;
  }

  type download_reader = {
    body : string;
    mutable offset : int;
    mutable read_fault : Awskit.Error.t option;
  }

  let now = now
  let region _ = Awskit.Region.of_string_exn "us-east-1"
  let credentials t = Ok t.credentials
  let endpoint _ = None
  let retry_policy _ = Awskit.Retry.default
  let sleep t span = Clock.advance t.store.clock span
  let s3_endpoint_config _ = default_endpoint_config

  let descriptor_for_string body =
    {
      Awskit.Body.Upload.content_length =
        Some (Int64.of_int (String.length body));
      payload_hash = Awskit.Body.Payload_hash.sha256_of_string body;
      replayable = true;
    }

  let empty_body = { descriptor = descriptor_for_string ""; body = Ok "" }

  let string_body value =
    { descriptor = descriptor_for_string value; body = Ok value }

  let bytes_body value = string_body (Bytes.to_string value)
  let body_error message = Awskit.Error.body message

  let writer_for descriptor =
    {
      buffer = Buffer.create 1024;
      remaining = ref descriptor.Awskit.Body.Upload.content_length;
      write_error = None;
    }

  let check_write_length writer value =
    match !(writer.remaining) with
    | None -> Ok ()
    | Some remaining ->
        let length = Int64.of_int (String.length value) in
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
        | None | Some 0L -> Ok (Buffer.contents writer.buffer)
        | Some _ ->
            Error
              (body_error "upload body ended before declared content_length"))

  let stream_body descriptor ~write =
    let writer = writer_for descriptor in
    let body =
      match write writer with
      | Ok () -> check_finished_length writer
      | Error _ as error -> error
    in
    { descriptor; body }

  let upload_descriptor (body : upload_body) = body.descriptor
  let upload_body_result (body : upload_body) = body.body

  let write_string writer value =
    match writer.write_error with
    | Some error -> Error error
    | None -> (
        match check_write_length writer value with
        | Error error ->
            writer.write_error <- Some error;
            Error error
        | Ok () ->
            Buffer.add_string writer.buffer value;
            Ok ())

  let read (reader : download_reader) bytes ~off ~len =
    match reader.read_fault with
    | Some error ->
        reader.read_fault <- None;
        Error error
    | None ->
        if len = 0 then Ok 0
        else
          let remaining = String.length reader.body - reader.offset in
          if remaining <= 0 then Ok 0
          else
            let copied = min len remaining in
            String.blit reader.body reader.offset bytes off copied;
            reader.offset <- reader.offset + copied;
            Ok copied

  let download_body ?read_fault body : download_body = { body; read_fault }

  let rec discard_reader reader =
    let buffer = Bytes.create 8192 in
    match read reader buffer ~off:0 ~len:(Bytes.length buffer) with
    | Error _ as error -> error
    | Ok 0 -> Ok ()
    | Ok _ -> discard_reader reader

  let with_download_body (body : download_body) ~consume =
    let reader =
      { body = body.body; offset = 0; read_fault = body.read_fault }
    in
    match consume reader with
    | Ok _ as result -> (
        match discard_reader reader with
        | Ok () -> result
        | Error _ as error -> error)
    | Error _ as error -> (
        match discard_reader reader with
        | Ok () -> error
        | Error _ as drain_error -> drain_error)

  let discard_download_body body =
    with_download_body body ~consume:(fun reader -> discard_reader reader)

  let with_response _ _ _ ~f:_ =
    Error
      (Awskit.Error.transport ~retryable:false
         "Sim.Runtime.with_response is not an HTTP transport")
end

type object_meta = {
  etag : Object.Etag.t option;
  size : int64 option;
  last_modified : Ptime.t option;
}

let object_meta store ~bucket ~key =
  match bucket_state store bucket with
  | None -> None
  | Some bucket -> (
      match Hashtbl.find_opt bucket.objects key with
      | None | Some (Stored_delete_marker _) -> None
      | Some (Stored_object obj) ->
          Some
            {
              etag = Some obj.etag;
              size = Some (Int64.of_int (String.length obj.body));
              last_modified = Some obj.last_modified;
            })

let keys store ~bucket =
  match bucket_state store bucket with
  | None -> []
  | Some bucket ->
      Hashtbl.to_seq_keys bucket.objects
      |> Seq.filter (fun key ->
          match Hashtbl.find_opt bucket.objects key with
          | Some (Stored_object _) -> true
          | None | Some (Stored_delete_marker _) -> false)
      |> List.of_seq
      |> List.sort String.compare

let history store = List.rev store.history
let clear_history store = store.history <- []

let dump_strings store ~bucket =
  match bucket_state store bucket with
  | None -> []
  | Some (bucket : bucket_state) ->
      Hashtbl.to_seq bucket.objects
      |> Seq.filter_map (function
        | key, Stored_object obj -> Some (key, obj.body)
        | _, Stored_delete_marker _ -> None)
      |> List.of_seq
      |> List.sort compare

let inject_fault t fault = t.faults <- t.faults @ [ fault ]
let inject_faults t faults = t.faults <- t.faults @ faults
let clear_faults t = t.faults <- []

let enable_buggify t ~seed ~prob =
  t.buggify <- Some { random = Random.State.make [| seed |]; prob }

let disable_buggify t = t.buggify <- None

let info_of_object ?content_length response (obj : stored_object) =
  {
    Object.Get.etag = Some obj.etag;
    content_type = obj.content_type;
    content_length =
      Some
        (Option.value ~default:(String.length obj.body) content_length
        |> Int64.of_int);
    last_modified = Some obj.last_modified;
    metadata = obj.metadata;
    storage_class = obj.storage_class;
    version_id = obj.version_id;
    checksum = obj.checksum;
    server_side_encryption = None;
    request = response;
  }
