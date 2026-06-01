module Runtime = struct
  type connection = {
    download_body : string;
    mutable uploaded_body : string option;
  }

  type 'a t = 'a Lwt.t

  type upload_writer = {
    buffer : Buffer.t;
    fail_after_bytes : int option;
    mutable written : int;
  }

  type upload_body =
    | Body of Awskit.Body.Upload.descriptor * (string, Awskit_s3.Error.t) result
    | Stream of
        Awskit.Body.Upload.descriptor
        * (upload_writer -> (unit, Awskit_s3.Error.t) result Lwt.t)

  type download_body = string
  type download_reader = { body : string; mutable offset : int }

  let write_error_after_bytes = ref None
  let reset_write_fault () = write_error_after_bytes := None
  let return = Lwt.return
  let bind = Lwt.bind
  let now _ = Ptime.epoch
  let region _ = Awskit.Region.of_string_exn "us-east-1"

  let credentials _ =
    Lwt.return_ok
      (Awskit.Credentials.create_exn ~access_key_id:"AK" ~secret_access_key:"SK"
         ())

  let endpoint _ = None
  let retry_policy _ = Awskit.Retry.default
  let sleep _ _ = Lwt.return_unit
  let s3_endpoint_config _ = Awskit_s3.default_endpoint_config

  let descriptor_for_string body =
    {
      Awskit.Body.Upload.content_length =
        Some (Int64.of_int (String.length body));
      payload_hash = Awskit.Body.Payload_hash.sha256_of_string body;
      replayable = true;
    }

  let empty_body = Body (descriptor_for_string "", Ok "")
  let string_body value = Body (descriptor_for_string value, Ok value)
  let bytes_body value = string_body (Bytes.to_string value)
  let stream_body descriptor ~write = Stream (descriptor, write)

  let drain_upload_body = function
    | Body (_, body) -> Lwt.return body
    | Stream (_, write) ->
        let writer =
          {
            buffer = Buffer.create 1024;
            fail_after_bytes = !write_error_after_bytes;
            written = 0;
          }
        in
        Lwt.bind (write writer) (function
          | Ok () -> Lwt.return_ok (Buffer.contents writer.buffer)
          | Error _ as error -> Lwt.return error)

  let upload_descriptor = function
    | Body (descriptor, _) | Stream (descriptor, _) -> descriptor

  let write_string writer value =
    let length = String.length value in
    match writer.fail_after_bytes with
    | Some limit when writer.written + length > limit ->
        Lwt.return_error (Awskit.Error.body "simulated upload write failure")
    | _ ->
        Buffer.add_string writer.buffer value;
        writer.written <- writer.written + length;
        Lwt.return_ok ()

  let read reader bytes ~off ~len =
    if len = 0 then Lwt.return_ok 0
    else
      let remaining = String.length reader.body - reader.offset in
      if remaining <= 0 then Lwt.return_ok 0
      else
        let copied = min len remaining in
        String.blit reader.body reader.offset bytes off copied;
        reader.offset <- reader.offset + copied;
        Lwt.return_ok copied

  let with_download_body body ~consume = consume { body; offset = 0 }
  let discard_download_body _ = Lwt.return_ok ()

  let with_response _ _ _ ~f:_ =
    Lwt.return_error (Awskit.Error.transport ~retryable:false "not implemented")
end

let unsupported () =
  Lwt.return_error (Awskit.Error.validation ~field:"test" "not implemented")

module S3 = struct
  module Object = struct
    type connection = Runtime.connection
    type 'a io = 'a Lwt.t
    type upload_body = Runtime.upload_body
    type download_reader = Runtime.download_reader

    let put conn ~bucket:_ ~key:_ ?options:_ ~body () =
      Lwt.bind (Runtime.drain_upload_body body) (function
        | Error _ as error -> Lwt.return error
        | Ok body ->
            conn.Runtime.uploaded_body <- Some body;
            let request = Awskit.Response.create_exn ~status:200 () in
            Lwt.return_ok
              {
                Awskit_s3.Object.Put.etag = None;
                version_id = None;
                checksum = None;
                request;
              })

    let get conn ~bucket:_ ~key:_ ?options:_ ~consume () =
      Lwt.bind
        (consume { Runtime.body = conn.Runtime.download_body; offset = 0 })
        (function
          | Error _ as error -> Lwt.return error
          | Ok value ->
              let request = Awskit.Response.create_exn ~status:200 () in
              Lwt.return_ok
                ( {
                    Awskit_s3.Object.Get.etag = None;
                    content_type = None;
                    content_length =
                      Some
                        (Int64.of_int
                           (String.length conn.Runtime.download_body));
                    last_modified = None;
                    metadata = [];
                    storage_class = None;
                    version_id = None;
                    checksum = None;
                    server_side_encryption = None;
                    request;
                  },
                  value ))

    let head _ ~bucket:_ ~key:_ ?options:_ () = unsupported ()
    let exists _ ~bucket:_ ~key:_ = unsupported ()
    let delete _ ~bucket:_ ~key:_ ?options:_ () = unsupported ()
    let delete_many _ ~bucket:_ ~objects:_ = unsupported ()

    let copy _ ~src_bucket:_ ~src_key:_ ~dst_bucket:_ ~dst_key:_ ?options:_ () =
      unsupported ()

    let list_versions _ ~bucket:_ ?options:_ () = unsupported ()
    let list _ ~bucket:_ ?options:_ () = unsupported ()
    let list_keys _ ~bucket:_ ?options:_ () = unsupported ()

    module Paginator = struct
      let fold_pages _ ~bucket:_ ?options:_ ?max_pages:_ ~init:_ ~f:_ () =
        unsupported ()

      let pages _ ~bucket:_ ?options:_ ?max_pages:_ () = unsupported ()
      let objects _ ~bucket:_ ?options:_ ?max_pages:_ () = unsupported ()
      let keys _ ~bucket:_ ?options:_ ?max_pages:_ () = unsupported ()
    end

    module Versions = struct
      let fold_pages _ ~bucket:_ ?options:_ ?max_pages:_ ~init:_ ~f:_ () =
        unsupported ()

      let pages _ ~bucket:_ ?options:_ ?max_pages:_ () = unsupported ()

      let object_versions _ ~bucket:_ ?options:_ ?max_pages:_ () =
        unsupported ()

      let delete_markers _ ~bucket:_ ?options:_ ?max_pages:_ () = unsupported ()
    end

    module Buffer = struct
      let put_string _ ~bucket:_ ~key:_ ?options:_ _ = unsupported ()
      let put_bytes _ ~bucket:_ ~key:_ ?options:_ _ = unsupported ()

      let get_string _ ~bucket:_ ~key:_ ~max_size:_ ?options:_ () =
        unsupported ()

      let get_bytes _ ~bucket:_ ~key:_ ~max_size:_ ?options:_ () =
        unsupported ()
    end

    module Tagging = struct
      let get _ ~bucket:_ ~key:_ = unsupported ()
      let put _ ~bucket:_ ~key:_ _ = unsupported ()
      let delete _ ~bucket:_ ~key:_ = unsupported ()
    end
  end

  module Multipart = struct
    type connection = Runtime.connection
    type 'a io = 'a Lwt.t
    type upload_body = Runtime.upload_body

    let create _ ~bucket:_ ~key:_ ?options:_ () = unsupported ()

    let upload_part _ ~bucket:_ ~key:_ ~upload_id:_ ~part_number:_ ~body:_
        ?options:_ () =
      unsupported ()

    let complete _ ~bucket:_ ~key:_ ~upload_id:_ _ = unsupported ()
    let abort _ ~bucket:_ ~key:_ ~upload_id:_ = unsupported ()

    let list_parts _ ~bucket:_ ~key:_ ~upload_id:_ ?options:_ () =
      unsupported ()

    module Paginator = struct
      let fold_pages _ ~bucket:_ ~key:_ ~upload_id:_ ?options:_ ?max_pages:_
          ~init:_ ~f:_ () =
        unsupported ()

      let pages _ ~bucket:_ ~key:_ ~upload_id:_ ?options:_ ?max_pages:_ () =
        unsupported ()

      let parts _ ~bucket:_ ~key:_ ~upload_id:_ ?options:_ ?max_pages:_ () =
        unsupported ()
    end

    module Managed = struct
      let upload_string _ ~bucket:_ ~key:_ ?options:_ _ = unsupported ()
      let upload_bytes _ ~bucket:_ ~key:_ ?options:_ _ = unsupported ()
    end
  end
end

module Transfer = Awskit_s3_lwt_unix__Transfer.Make (Runtime) (S3)

let remove_file path = try Sys.remove path with Sys_error _ -> ()

let with_umask mask f =
  let previous = Unix.umask mask in
  Fun.protect ~finally:(fun () -> ignore (Unix.umask previous)) f

let test_download_to_path_creates_private_file () =
  let path = Filename.temp_file "awskit-download-perm" ".bin" in
  let body = "secret download body" in
  remove_file path;
  Fun.protect
    ~finally:(fun () -> remove_file path)
    (fun () ->
      with_umask 0 (fun () ->
          match
            Lwt_main.run
              (Transfer.download_to_path
                 { Runtime.download_body = body; uploaded_body = None }
                 ~bucket:"bucket" ~key:"key" ~path ())
          with
          | Error error ->
              Alcotest.failf "download failed: %a" Awskit_s3.Error.pp error
          | Ok _ -> ());
      let stat = Unix.stat path in
      Alcotest.(check int) "mode" 0o600 (stat.Unix.st_perm land 0o777))

let write_file path body =
  let channel = open_out_bin path in
  Fun.protect
    ~finally:(fun () -> close_out_noerr channel)
    (fun () -> output_string channel body)

let test_upload_from_path_streams_file_body () =
  Runtime.reset_write_fault ();
  let path = Filename.temp_file "awskit-upload" ".bin" in
  let body = "first line\nsecond line\n" in
  write_file path body;
  Fun.protect
    ~finally:(fun () -> remove_file path)
    (fun () ->
      let conn = { Runtime.download_body = ""; uploaded_body = None } in
      let progress = ref [] in
      match
        Lwt_main.run
          (Transfer.upload_from_path conn ~bucket:"bucket" ~key:"key" ~path
             ~on_progress:(fun transferred ->
               progress := transferred :: !progress)
             ())
      with
      | Error error ->
          Alcotest.failf "upload failed: %a" Awskit_s3.Error.pp error
      | Ok _ ->
          Alcotest.(check (option string))
            "uploaded body" (Some body) conn.uploaded_body;
          Alcotest.(check (list int64))
            "progress"
            [ Int64.of_int (String.length body) ]
            (List.rev !progress))

let test_upload_from_path_returns_stream_write_error () =
  Runtime.write_error_after_bytes := Some 0;
  let path = Filename.temp_file "awskit-upload-error" ".bin" in
  write_file path "body that cannot be written";
  Fun.protect
    ~finally:(fun () ->
      Runtime.reset_write_fault ();
      remove_file path)
    (fun () ->
      let conn = { Runtime.download_body = ""; uploaded_body = None } in
      match
        Lwt_main.run
          (Transfer.upload_from_path conn ~bucket:"bucket" ~key:"key" ~path ())
      with
      | Ok _ -> Alcotest.fail "upload succeeded despite write failure"
      | Error error ->
          Alcotest.(check (option string))
            "no stored body" None conn.uploaded_body;
          Alcotest.(check string)
            "error" "body: simulated upload write failure"
            (Fmt.str "%a" Awskit_s3.Error.pp error))

let suite () =
  [
    ( "transfer",
      [
        Alcotest.test_case "download creates private file" `Quick
          test_download_to_path_creates_private_file;
        Alcotest.test_case "upload streams file body" `Quick
          test_upload_from_path_streams_file_body;
        Alcotest.test_case "upload returns stream write error" `Quick
          test_upload_from_path_returns_stream_write_error;
      ] );
  ]
