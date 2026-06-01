(** Integration tests for Eio runtime: connection creation. *)

open Base

let test_connection_roundtrip env =
  let c =
    Awskit.Credentials.create_exn ~access_key_id:"AK" ~secret_access_key:"SK" ()
  in
  let clock () = Ptime.epoch in
  let region = Awskit.Region.of_string_exn "eu-west-1" in
  let endpoint = Awskit.Endpoint.http_exn ~host:"localhost" ~port:9000 () in
  let conn =
    Eio.Switch.run @@ fun sw ->
    Awskit_eio.create ~env ~sw ~region ~credentials:c ~clock ~endpoint ()
  in
  Alcotest.(check string)
    "region" "eu-west-1"
    (Awskit_eio.Runtime.region conn |> Awskit.Region.to_string);
  Alcotest.(check (option string))
    "endpoint" (Some "http://localhost:9000")
    (Option.map
       Awskit_eio.Runtime.(endpoint conn)
       ~f:Awskit.Endpoint.to_url_prefix)

let test_connection_defaults env =
  let c =
    Awskit.Credentials.create_exn ~access_key_id:"AK" ~secret_access_key:"SK" ()
  in
  let clock () = Ptime.epoch in
  let region = Awskit.Region.of_string_exn "us-east-1" in
  let conn =
    Eio.Switch.run @@ fun sw ->
    Awskit_eio.create ~env ~sw ~region ~credentials:c ~clock ()
  in
  Alcotest.(check (option string))
    "no endpoint" None
    (Option.map
       Awskit_eio.Runtime.(endpoint conn)
       ~f:Awskit.Endpoint.to_url_prefix)

let test_runtime_bodies env =
  let c =
    Awskit.Credentials.create_exn ~access_key_id:"AK" ~secret_access_key:"SK" ()
  in
  let region = Awskit.Region.of_string_exn "us-east-1" in
  Eio.Switch.run @@ fun sw ->
  let conn = Awskit_eio.create ~env ~sw ~region ~credentials:c () in
  ignore (Awskit_eio.Runtime.region conn : Awskit.Region.t);
  let body = Awskit_eio.Runtime.string_body "hello" in
  Alcotest.(check int64)
    "content length" 5L
    (Option.value (Awskit_eio.Runtime.upload_descriptor body).content_length
       ~default:(-1L))

let stream_descriptor length =
  {
    Awskit.Body.Upload.content_length = Some length;
    payload_hash = Awskit.Body.Payload_hash.unsigned_payload;
    replayable = false;
  }

let upload_conn env sw =
  let credentials =
    Awskit.Credentials.create_exn ~access_key_id:"AK" ~secret_access_key:"SK" ()
  in
  let region = Awskit.Region.of_string_exn "us-east-1" in
  Awskit_eio.create ~env ~sw ~region ~credentials
    ~clock:(fun () -> Ptime.epoch)
    ()

let body_to_cohttp_exn conn body =
  let bridge =
    Awskit_eio__Runtime.body_to_cohttp ~sw:conn.Awskit_eio__Runtime.sw body
  in
  match bridge.body with
  | Some body -> (body, bridge.finished)
  | None -> Alcotest.fail "expected upload body"

let rec read_all body buffer =
  let chunk = Bytes.create 2 in
  match
    Awskit_eio__Runtime.read body chunk ~off:0 ~len:(Bytes.length chunk)
  with
  | Error error ->
      Alcotest.failf "unexpected body read error: %a" Awskit.Error.pp error
  | Ok 0 -> Buffer.contents buffer
  | Ok n ->
      Buffer.add_substring buffer (Bytes.to_string chunk) ~pos:0 ~len:n;
      read_all body buffer

let rec read_all_cohttp_body body buffer =
  let chunk = Cstruct.create 2 in
  match Eio.Flow.single_read body chunk with
  | n ->
      Buffer.add_substring buffer (Cstruct.to_string chunk) ~pos:0 ~len:n;
      read_all_cohttp_body body buffer
  | exception End_of_file -> Buffer.contents buffer

let with_eio_early_response_server env ~status ~response_body ~read_request_body
    test =
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let listening_socket =
    Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:1
      (`Tcp (Eio.Net.Ipaddr.V4.loopback, 0))
  in
  let port =
    match Eio.Net.listening_addr listening_socket with
    | `Tcp (_, port) -> port
    | _ -> Alcotest.fail "expected TCP listening socket"
  in
  Eio.Fiber.fork ~sw (fun () ->
      Eio.Net.accept_fork listening_socket ~sw
        ~on_error:(fun exn -> raise exn)
        (fun flow _addr ->
          let input = Eio.Buf_read.of_flow ~max_size:Int.max_value flow in
          let rec read_headers () =
            match Eio.Buf_read.line input with
            | "" -> []
            | line -> line :: read_headers ()
          in
          let headers = read_headers () in
          let content_length =
            List.find_map headers ~f:(fun header ->
                match String.lsplit2 header ~on:':' with
                | Some (name, value)
                  when String.Caseless.equal (String.strip name)
                         "content-length" -> (
                    match Int.of_string_opt (String.strip value) with
                    | Some length when length >= 0 -> Some length
                    | _ ->
                        Alcotest.failf "invalid Content-Length header: %s"
                          header)
                | _ -> None)
          in
          let () =
            if read_request_body then
              match content_length with
              | Some length -> ignore (Eio.Buf_read.take length input : string)
              | None ->
                  Alcotest.fail
                    "read_request_body requires a Content-Length header"
          in
          Eio.Buf_write.with_flow flow (fun output ->
              Eio.Buf_write.string output
                (Fmt.str
                   "HTTP/1.1 %d test\r\n\
                    Content-Length: %d\r\n\
                    Connection: close\r\n\
                    \r\n\
                    %s"
                   status
                   (String.length response_body)
                   response_body);
              Eio.Buf_write.flush output)));
  let endpoint = Awskit.Endpoint.http_exn ~host:"127.0.0.1" ~port () in
  test endpoint

let upload_request_for_endpoint endpoint =
  let target =
    Awskit.Request.Target.create_exn
      ~scheme:(Awskit.Endpoint.scheme endpoint)
      ~host:(Awskit.Endpoint.host endpoint)
      ?port:(Awskit.Endpoint.port endpoint)
      ~path:"/" ()
  in
  Awskit.Request.create_exn ~method_:`PUT ~target ()

let read_download_body_to_string body =
  Awskit_eio__Runtime.with_download_body body ~consume:(fun reader ->
      Ok (read_all reader (Buffer.create 128)))

let test_stream_upload_body_emits_multiple_chunks env =
  Eio.Switch.run @@ fun sw ->
  let conn = upload_conn env sw in
  let body =
    Awskit_eio__Runtime.stream_body (stream_descriptor 6L) ~write:(fun writer ->
        match Awskit_eio__Runtime.write_string writer "ab" with
        | Error _ as error -> error
        | Ok () -> (
            match Awskit_eio__Runtime.write_string writer "cd" with
            | Error _ as error -> error
            | Ok () -> Awskit_eio__Runtime.write_string writer "ef"))
  in
  let cohttp_body, upload_finished = body_to_cohttp_exn conn body in
  Alcotest.(check string)
    "request body" "abcdef"
    (read_all_cohttp_body cohttp_body (Buffer.create 6));
  match Eio.Promise.await upload_finished with
  | Ok () -> ()
  | Error error ->
      Alcotest.failf "unexpected upload error: %a" Awskit.Error.pp error

let test_stream_upload_error_propagates env =
  Eio.Switch.run @@ fun sw ->
  let conn = upload_conn env sw in
  let stream_error = Awskit.Error.body "stream upload failed" in
  let body =
    Awskit_eio__Runtime.stream_body (stream_descriptor 4L) ~write:(fun writer ->
        match Awskit_eio__Runtime.write_string writer "ab" with
        | Error _ as error -> error
        | Ok () -> Error stream_error)
  in
  let _cohttp_body, upload_finished = body_to_cohttp_exn conn body in
  match Eio.Promise.await upload_finished with
  | Error error when Awskit.Error.equal error stream_error -> ()
  | Error error ->
      Alcotest.failf "unexpected upload error: %a" Awskit.Error.pp error
  | Ok () -> Alcotest.fail "expected stream upload error"

let expect_upload_body_error label = function
  | Error (Awskit.Error.Body _) -> ()
  | Error error ->
      Alcotest.failf "%s: unexpected error: %a" label Awskit.Error.pp error
  | Ok () -> Alcotest.failf "%s: expected upload body error" label

let test_stream_upload_rejects_short_body env =
  Eio.Switch.run @@ fun sw ->
  let conn = upload_conn env sw in
  let body =
    Awskit_eio__Runtime.stream_body (stream_descriptor 4L) ~write:(fun writer ->
        Awskit_eio__Runtime.write_string writer "ab")
  in
  let _cohttp_body, upload_finished = body_to_cohttp_exn conn body in
  Eio.Promise.await upload_finished
  |> expect_upload_body_error "short upload body"

let test_stream_upload_rejects_long_body env =
  Eio.Switch.run @@ fun sw ->
  let conn = upload_conn env sw in
  let body =
    Awskit_eio__Runtime.stream_body (stream_descriptor 4L) ~write:(fun writer ->
        match Awskit_eio__Runtime.write_string writer "abcd" with
        | Error _ as error -> error
        | Ok () -> Awskit_eio__Runtime.write_string writer "e")
  in
  let _cohttp_body, upload_finished = body_to_cohttp_exn conn body in
  Eio.Promise.await upload_finished
  |> expect_upload_body_error "long upload body"

let test_stream_upload_early_response_cancels_producer_and_preserves_body env =
  let error_body =
    "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>"
  in
  with_eio_early_response_server env ~status:403 ~response_body:error_body
    ~read_request_body:false (fun endpoint ->
      Eio.Switch.run @@ fun conn_sw ->
      let clock = Eio.Stdenv.clock env in
      let lifecycle_timeout = 1.0 in
      let conn = upload_conn env conn_sw in
      let request = upload_request_for_endpoint endpoint in
      let producer_started = ref false in
      let producer_finished, wake_producer_finished = Eio.Promise.create () in
      let finish_producer () =
        ignore (Eio.Promise.try_resolve wake_producer_finished () : bool)
      in
      let body =
        Awskit_eio__Runtime.stream_body (stream_descriptor 1024L)
          ~write:(fun writer ->
            producer_started := true;
            match Awskit_eio__Runtime.write_string writer "ab" with
            | Error _ as error -> error
            | Ok () ->
                Exn.protect ~finally:finish_producer ~f:(fun () ->
                    Eio.Fiber.await_cancel ()))
      in
      let result =
        try
          Eio.Time.with_timeout_exn clock lifecycle_timeout (fun () ->
              Awskit_eio__Runtime.with_response conn request body
                ~f:(fun response response_body ->
                  match read_download_body_to_string response_body with
                  | Ok body -> Ok (response, body)
                  | Error _ as error -> error))
        with Eio.Time.Timeout ->
          Alcotest.fail
            "Runtime.with_response did not return after early service response"
      in
      Alcotest.(check bool) "producer started" true !producer_started;
      (try
         Eio.Time.with_timeout_exn clock lifecycle_timeout (fun () ->
             Eio.Promise.await producer_finished)
       with Eio.Time.Timeout ->
         Alcotest.fail
           "producer was still alive after Runtime.with_response returned");
      match result with
      | Error error ->
          Alcotest.failf "unexpected runtime error: %a" Awskit.Error.pp error
      | Ok (response, body) ->
          Alcotest.(check int) "status" 403 (Awskit.Response.status response);
          Alcotest.(check string) "error body" error_body body)

let test_stream_upload_success_response_body_is_scoped env =
  let response_body = String.make (128 * 1024) 'r' in
  with_eio_early_response_server env ~status:200 ~response_body
    ~read_request_body:false (fun endpoint ->
      Eio.Switch.run @@ fun conn_sw ->
      let conn = upload_conn env conn_sw in
      let request = upload_request_for_endpoint endpoint in
      let body =
        Awskit_eio__Runtime.stream_body (stream_descriptor 2L)
          ~write:(fun writer -> Awskit_eio__Runtime.write_string writer "ok")
      in
      let result =
        Awskit_eio__Runtime.with_response conn request body
          ~f:(fun response response_body ->
            match read_download_body_to_string response_body with
            | Ok body -> Ok (response, body)
            | Error _ as error -> error)
      in
      match result with
      | Error error ->
          Alcotest.failf "unexpected runtime error: %a" Awskit.Error.pp error
      | Ok (response, body) ->
          Alcotest.(check int) "status" 200 (Awskit.Response.status response);
          Alcotest.(check int)
            "response length"
            (String.length response_body)
            (String.length body);
          Alcotest.(check string) "response body" response_body body)

let eio_download_body ~max_response_body_bytes body =
  {
    Awskit_eio__Runtime.body = Cohttp_eio.Body.of_string body;
    max_response_body_bytes;
  }

let expect_body_limit label expected = function
  | Error (Awskit.Error.Body { limit = Some limit; _ }) ->
      Alcotest.(check int64) label expected limit
  | Error error ->
      Alcotest.failf "%s: unexpected error: %a" label Awskit.Error.pp error
  | Ok _ -> Alcotest.failf "%s: expected body limit error" label

let test_discard_download_body_enforces_limit _env =
  eio_download_body ~max_response_body_bytes:3 "abcdef"
  |> Awskit_eio__Runtime.discard_download_body
  |> expect_body_limit "discard limit" 3L

let test_with_download_body_drain_enforces_limit _env =
  let body = eio_download_body ~max_response_body_bytes:3 "abcdef" in
  Awskit_eio__Runtime.with_download_body body ~consume:(fun _ -> Ok ())
  |> expect_body_limit "scoped drain limit" 3L

let suite env =
  [
    ( "integration:connection",
      [
        Alcotest.test_case "roundtrip" `Quick (fun () ->
            test_connection_roundtrip env);
        Alcotest.test_case "defaults" `Quick (fun () ->
            test_connection_defaults env);
        Alcotest.test_case "runtime bodies" `Quick (fun () ->
            test_runtime_bodies env);
        Alcotest.test_case "stream upload emits multiple chunks" `Quick
          (fun () -> test_stream_upload_body_emits_multiple_chunks env);
        Alcotest.test_case "stream upload error propagates" `Quick (fun () ->
            test_stream_upload_error_propagates env);
        Alcotest.test_case "stream upload rejects short body" `Quick (fun () ->
            test_stream_upload_rejects_short_body env);
        Alcotest.test_case "stream upload rejects long body" `Quick (fun () ->
            test_stream_upload_rejects_long_body env);
        Alcotest.test_case
          "stream upload early response cancels producer and preserves body"
          `Quick (fun () ->
            test_stream_upload_early_response_cancels_producer_and_preserves_body
              env);
        Alcotest.test_case "stream upload success response body is scoped"
          `Quick (fun () ->
            test_stream_upload_success_response_body_is_scoped env);
        Alcotest.test_case "discard body limit" `Quick (fun () ->
            test_discard_download_body_enforces_limit env);
        Alcotest.test_case "scoped drain body limit" `Quick (fun () ->
            test_with_download_body_drain_enforces_limit env);
      ] );
  ]
