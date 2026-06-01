(** Integration tests for the generic Lwt runtime adapter. *)

open Base
module Aws = Awskit_lwt.Make (Cohttp_lwt_unix.Client)

module Limited_body_client = struct
  type ctx = unit

  module IO = Cohttp_lwt_unix.Client.IO

  type 'a io = 'a Lwt.t
  type 'a with_context = ?ctx:ctx -> 'a
  type body = Cohttp_lwt.Body.t

  let response_body = ref ""
  let response () = Cohttp.Response.make ~status:`OK ()
  let body () = Cohttp_lwt.Body.of_string !response_body
  let map_context f g ?ctx = g (f ?ctx)

  let call ?ctx:_ ?headers:_ ?body:_ ?chunked:_ _meth _uri =
    Lwt.return (response (), body ())

  let head ?ctx:_ ?headers:_ _uri = Lwt.return (response ())
  let get ?ctx ?headers uri = call ?ctx ?headers `GET uri

  let delete ?ctx ?body ?chunked ?headers uri =
    call ?ctx ?body ?chunked ?headers `DELETE uri

  let post ?ctx ?body ?chunked ?headers uri =
    call ?ctx ?body ?chunked ?headers `POST uri

  let put ?ctx ?body ?chunked ?headers uri =
    call ?ctx ?body ?chunked ?headers `PUT uri

  let patch ?ctx ?body ?chunked ?headers uri =
    call ?ctx ?body ?chunked ?headers `PATCH uri

  let set_cache _ = ()

  let post_form ?ctx:_ ?headers:_ ~params:_ _uri =
    Lwt.return (response (), body ())

  let callv ?ctx:_ _uri _requests = Lwt.return (Lwt_stream.of_list [])
end

module LimitedAws = Awskit_lwt.Make (Limited_body_client)

module Upload_body_client = struct
  type ctx = unit

  module IO = Cohttp_lwt_unix.Client.IO

  type 'a io = 'a Lwt.t
  type 'a with_context = ?ctx:ctx -> 'a
  type body = Cohttp_lwt.Body.t

  let request_body = ref None
  let response () = Cohttp.Response.make ~status:`OK ()
  let empty_response_body () = Cohttp_lwt.Body.empty
  let map_context f g ?ctx = g (f ?ctx)

  let call ?ctx:_ ?headers:_ ?body ?chunked:_ _meth _uri =
    let body =
      match body with
      | None -> Lwt.return ""
      | Some body -> Cohttp_lwt.Body.to_string body
    in
    Lwt.bind body (fun request_body_value ->
        request_body := Some request_body_value;
        Lwt.return (response (), empty_response_body ()))

  let head ?ctx:_ ?headers:_ _uri = Lwt.return (response ())
  let get ?ctx ?headers uri = call ?ctx ?headers `GET uri

  let delete ?ctx ?body ?chunked ?headers uri =
    call ?ctx ?body ?chunked ?headers `DELETE uri

  let post ?ctx ?body ?chunked ?headers uri =
    call ?ctx ?body ?chunked ?headers `POST uri

  let put ?ctx ?body ?chunked ?headers uri =
    call ?ctx ?body ?chunked ?headers `PUT uri

  let patch ?ctx ?body ?chunked ?headers uri =
    call ?ctx ?body ?chunked ?headers `PATCH uri

  let set_cache _ = ()

  let post_form ?ctx:_ ?headers:_ ~params:_ _uri =
    Lwt.return (response (), empty_response_body ())

  let callv ?ctx:_ _uri _requests = Lwt.return (Lwt_stream.of_list [])
end

module UploadAws = Awskit_lwt.Make (Upload_body_client)

let test_connection_roundtrip () =
  let c =
    Awskit.Credentials.create_exn ~access_key_id:"AK" ~secret_access_key:"SK" ()
  in
  let clock () = Ptime.epoch in
  let region = Awskit.Region.of_string_exn "eu-west-1" in
  let endpoint = Awskit.Endpoint.http_exn ~host:"localhost" ~port:9000 () in
  let conn = Aws.create ~region ~credentials:c ~clock ~endpoint () in
  Alcotest.(check string)
    "region" "eu-west-1"
    (Aws.Runtime.region conn |> Awskit.Region.to_string);
  Alcotest.(check (option string))
    "endpoint" (Some "http://localhost:9000")
    (Option.map Aws.Runtime.(endpoint conn) ~f:Awskit.Endpoint.to_url_prefix)

let test_connection_defaults () =
  let c =
    Awskit.Credentials.create_exn ~access_key_id:"AK" ~secret_access_key:"SK" ()
  in
  let clock () = Ptime.epoch in
  let region = Awskit.Region.of_string_exn "us-east-1" in
  let conn = Aws.create ~region ~credentials:c ~clock () in
  Alcotest.(check (option string))
    "no endpoint" None
    (Option.map Aws.Runtime.(endpoint conn) ~f:Awskit.Endpoint.to_url_prefix)

let test_runtime_bodies () =
  let body = Aws.Runtime.string_body "hello" in
  Alcotest.(check int64)
    "content length" 5L
    (Option.value (Aws.Runtime.upload_descriptor body).content_length
       ~default:(-1L))

let upload_conn () =
  let credentials =
    Awskit.Credentials.create_exn ~access_key_id:"AK" ~secret_access_key:"SK" ()
  in
  let region = Awskit.Region.of_string_exn "us-east-1" in
  UploadAws.create ~region ~credentials ~clock:(fun () -> Ptime.epoch) ()

let upload_request =
  let target =
    Awskit.Request.Target.create_exn ~scheme:`Http ~host:"localhost" ~path:"/"
      ()
  in
  Awskit.Request.create_exn ~method_:`PUT ~target ()

let stream_descriptor length =
  {
    Awskit.Body.Upload.content_length = Some length;
    payload_hash = Awskit.Body.Payload_hash.unsigned_payload;
    replayable = false;
  }

let test_stream_upload_body_reaches_client () =
  Upload_body_client.request_body := None;
  let body =
    UploadAws.Runtime.stream_body (stream_descriptor 4L) ~write:(fun writer ->
        Lwt.bind (UploadAws.Runtime.write_string writer "ab") (function
          | Error _ as error -> Lwt.return error
          | Ok () -> UploadAws.Runtime.write_string writer "cd"))
  in
  match
    Lwt_main.run
      (UploadAws.Runtime.with_response (upload_conn ()) upload_request body
         ~f:(fun _ body -> UploadAws.Runtime.discard_download_body body))
  with
  | Error error ->
      Alcotest.failf "unexpected upload error: %a" Awskit.Error.pp error
  | Ok () ->
      Alcotest.(check (option string))
        "request body" (Some "abcd")
        !Upload_body_client.request_body

let test_stream_upload_error_propagates () =
  Upload_body_client.request_body := None;
  let stream_error = Awskit.Error.body "stream upload failed" in
  let body =
    UploadAws.Runtime.stream_body (stream_descriptor 4L) ~write:(fun writer ->
        Lwt.bind (UploadAws.Runtime.write_string writer "ab") (function
          | Error _ as error -> Lwt.return error
          | Ok () -> Lwt.return_error stream_error))
  in
  match
    Lwt_main.run
      (UploadAws.Runtime.with_response (upload_conn ()) upload_request body
         ~f:(fun _ body -> UploadAws.Runtime.discard_download_body body))
  with
  | Error error when Awskit.Error.equal error stream_error -> ()
  | Error error ->
      Alcotest.failf "unexpected upload error: %a" Awskit.Error.pp error
  | Ok () -> Alcotest.fail "expected stream upload error"

let expect_upload_body_error label result =
  match result with
  | Error (Awskit.Error.Body _) -> ()
  | Error error ->
      Alcotest.failf "%s: unexpected error: %a" label Awskit.Error.pp error
  | Ok _ -> Alcotest.failf "%s: expected upload body error" label

let test_stream_upload_rejects_short_body () =
  Upload_body_client.request_body := None;
  let body =
    UploadAws.Runtime.stream_body (stream_descriptor 4L) ~write:(fun writer ->
        UploadAws.Runtime.write_string writer "ab")
  in
  Lwt_main.run
    (UploadAws.Runtime.with_response (upload_conn ()) upload_request body
       ~f:(fun _ body -> UploadAws.Runtime.discard_download_body body))
  |> expect_upload_body_error "short upload body"

let test_stream_upload_rejects_long_body () =
  Upload_body_client.request_body := None;
  let body =
    UploadAws.Runtime.stream_body (stream_descriptor 4L) ~write:(fun writer ->
        Lwt.bind (UploadAws.Runtime.write_string writer "abcd") (function
          | Error _ as error -> Lwt.return error
          | Ok () -> UploadAws.Runtime.write_string writer "e"))
  in
  Lwt_main.run
    (UploadAws.Runtime.with_response (upload_conn ()) upload_request body
       ~f:(fun _ body -> UploadAws.Runtime.discard_download_body body))
  |> expect_upload_body_error "long upload body"

let limited_conn ~max_response_body_bytes =
  let credentials =
    Awskit.Credentials.create_exn ~access_key_id:"AK" ~secret_access_key:"SK" ()
  in
  let region = Awskit.Region.of_string_exn "us-east-1" in
  LimitedAws.create ~region ~credentials
    ~clock:(fun () -> Ptime.epoch)
    ~max_response_body_bytes ()

let limited_request =
  let target =
    Awskit.Request.Target.create_exn ~scheme:`Http ~host:"localhost" ~path:"/"
      ()
  in
  Awskit.Request.create_exn ~method_:`GET ~target ()

let with_limited_response ~max_response_body_bytes body ~f =
  Limited_body_client.response_body := body;
  let conn = limited_conn ~max_response_body_bytes in
  Lwt_main.run
    (LimitedAws.Runtime.with_response conn limited_request
       LimitedAws.Runtime.empty_body ~f:(fun _ body -> f body))

let expect_body_limit label expected = function
  | Error (Awskit.Error.Body { limit = Some limit; _ }) ->
      Alcotest.(check int64) label expected limit
  | Error error ->
      Alcotest.failf "%s: unexpected error: %a" label Awskit.Error.pp error
  | Ok _ -> Alcotest.failf "%s: expected body limit error" label

let test_discard_download_body_enforces_limit () =
  with_limited_response ~max_response_body_bytes:3 "abcdef"
    ~f:LimitedAws.Runtime.discard_download_body
  |> expect_body_limit "discard limit" 3L

let test_with_download_body_drain_enforces_limit () =
  with_limited_response ~max_response_body_bytes:3 "abcdef" ~f:(fun body ->
      LimitedAws.Runtime.with_download_body body ~consume:(fun _ ->
          Lwt.return_ok ()))
  |> expect_body_limit "scoped drain limit" 3L

let suite =
  [
    ( "integration:connection",
      [
        Alcotest.test_case "roundtrip" `Quick test_connection_roundtrip;
        Alcotest.test_case "defaults" `Quick test_connection_defaults;
        Alcotest.test_case "runtime bodies" `Quick test_runtime_bodies;
        Alcotest.test_case "stream upload reaches client" `Quick
          test_stream_upload_body_reaches_client;
        Alcotest.test_case "stream upload error propagates" `Quick
          test_stream_upload_error_propagates;
        Alcotest.test_case "stream upload rejects short body" `Quick
          test_stream_upload_rejects_short_body;
        Alcotest.test_case "stream upload rejects long body" `Quick
          test_stream_upload_rejects_long_body;
        Alcotest.test_case "discard body limit" `Quick
          test_discard_download_body_enforces_limit;
        Alcotest.test_case "scoped drain body limit" `Quick
          test_with_download_body_drain_enforces_limit;
      ] );
  ]
