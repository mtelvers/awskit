open Core

module Make (C : Operation_context.S) = struct
  open C

  let ( let* ) = bind

  type nonrec connection = connection
  type 'a io = 'a R.t
  type nonrec upload_body = upload_body

  let complete_result response body =
    match Xml.root body with
    | Error _ as error -> error
    | Ok ("Error", _) -> Error (embedded_service_error response body)
    | Ok ("CompleteMultipartUploadResult", nodes) -> (
        let etag = Xml.child_text "ETag" nodes in
        let etag = option_map_result Public_object.Etag.of_string etag in
        let version_id = response_version response in
        match (etag, version_id) with
        | Error error, _ | _, Error error -> Error error
        | Ok etag, Ok version_id ->
            Ok
              {
                Public_multipart.Complete.etag;
                version_id;
                checksum = response_checksum response;
                request = response;
              })
    | Ok (actual, _) ->
        Error
          (Awskit.Error.decode
             (Fmt.str "expected CompleteMultipartUploadResult XML, got %s"
                actual))

  let create conn ~bucket ~key ?options () =
    let options =
      Option.value ~default:Public_multipart.Create.default_options options
    in
    match validate_bucket_key bucket key with
    | Error error -> return_error error
    | Ok () -> (
        match validate_metadata options.metadata with
        | Error error -> return_error error
        | Ok () -> (
            match validate_tags options.tags with
            | Error error -> return_error error
            | Ok () -> (
                let headers =
                  Metadata_headers.to_headers options.metadata
                  @ checksum_request_headers options.checksum
                  @ encryption_request_headers options.server_side_encryption
                  |> add_opt_header "content-type" options.content_type
                  |> add_opt_header "x-amz-storage-class"
                       (Option.map Storage_class.to_string options.storage_class)
                  |> add_opt_header "x-amz-tagging" (tags_header options.tags)
                in
                match object_request conn ~bucket ~key with
                | Error error -> return_error error
                | Ok request ->
                    with_empty_response conn ~method_:`POST ~request
                      ~query:[ ("uploads", []) ]
                      ~headers
                      ~f:(fun response body ->
                        let* body =
                          read_download_body body ~max_size:1_048_576L
                        in
                        match body with
                        | Error error -> return_error error
                        | Ok body -> (
                            match
                              Xml.decode_root body
                                ~name:"InitiateMultipartUploadResult"
                            with
                            | Error error -> return_error error
                            | Ok nodes -> (
                                match Xml.child_text "UploadId" nodes with
                                | None ->
                                    return_error (decode "missing UploadId")
                                | Some upload_id -> (
                                    match
                                      Public_multipart.Upload_id.of_string
                                        upload_id
                                    with
                                    | Error error -> return_error error
                                    | Ok upload_id ->
                                        return_ok
                                          {
                                            Public_multipart.Create.upload =
                                              { bucket; key; upload_id };
                                            request = response;
                                          })))))))

  let upload_part conn ~bucket ~key ~upload_id ~part_number ~body ?options () =
    let options =
      Option.value ~default:Public_multipart.Upload_part.default_options options
    in
    match validate_bucket_key bucket key with
    | Error error -> return_error error
    | Ok () -> (
        match Public_multipart.Part.create ~part_number ~etag:"unused" with
        | Error error -> return_error error
        | Ok _ -> (
            let descriptor = R.upload_descriptor body in
            match descriptor.content_length with
            | None ->
                return_error
                  (Awskit.Error.validation ~field:"content_length"
                     "S3 multipart uploads require a known content length")
            | Some content_length -> (
                match Awskit.Body.Upload.validate_descriptor descriptor with
                | Error error -> return_error error
                | Ok () -> (
                    let headers =
                      ("content-length", Int64.to_string content_length)
                      :: checksum_request_headers options.checksum
                    in
                    let query =
                      [
                        ("partNumber", [ string_of_int part_number ]);
                        ( "uploadId",
                          [ Public_multipart.Upload_id.to_string upload_id ] );
                      ]
                    in
                    match object_request conn ~bucket ~key with
                    | Error error -> return_error error
                    | Ok request ->
                        with_response conn ~method_:`PUT ~request ~query
                          ~headers ~payload_hash:descriptor.payload_hash body
                          ~f:(fun response body ->
                            let* discarded = discard_download_body body in
                            match discarded with
                            | Error error -> return_error error
                            | Ok () -> (
                                match response_etag response with
                                | Error error -> return_error error
                                | Ok None ->
                                    return_error
                                      (decode "missing multipart part etag")
                                | Ok (Some etag) -> (
                                    match
                                      Public_multipart.Part.create ~part_number
                                        ~etag
                                    with
                                    | Error error -> return_error error
                                    | Ok part ->
                                        return_ok
                                          {
                                            Public_multipart.Upload_part.part;
                                            checksum =
                                              response_checksum response;
                                            request = response;
                                          })))))))

  let complete conn ~bucket ~key ~upload_id parts =
    match validate_bucket_key bucket key with
    | Error error -> return_error error
    | Ok () -> (
        let rec validate previous = function
          | [] -> Ok ()
          | (part : Public_multipart.Part.t) :: rest ->
              if part.part_number <= 0 then
                invalid ~field:"part_number" "part number must be positive"
              else if
                match previous with
                | Some prev -> part.part_number <= prev
                | None -> false
              then
                invalid ~field:"part_number"
                  "parts must be sorted by part_number"
              else validate (Some part.part_number) rest
        in
        match parts with
        | [] ->
            return_error
              (Awskit.Error.validation ~field:"parts"
                 "complete requires at least one part")
        | parts -> (
            match validate None parts with
            | Error error -> return_error error
            | Ok () -> (
                let body =
                  Xml.el "CompleteMultipartUpload"
                    (List.map
                       (fun (part : Public_multipart.Part.t) ->
                         Xml.el "Part"
                           [
                             Xml.text "PartNumber"
                               (string_of_int part.part_number);
                             Xml.text "ETag"
                               (Public_object.Etag.to_string part.etag);
                           ])
                       parts)
                  |> Xml.to_string
                in
                let upload = R.string_body body in
                match object_request conn ~bucket ~key with
                | Error error -> return_error error
                | Ok request ->
                    with_response conn ~method_:`POST ~request
                      ~query:
                        [
                          ( "uploadId",
                            [ Public_multipart.Upload_id.to_string upload_id ]
                          );
                        ]
                      ~headers:[ ("content-type", "application/xml") ]
                      ~payload_hash:(R.upload_descriptor upload).payload_hash
                      upload
                      ~f:(fun response body ->
                        let* body =
                          read_download_body body ~max_size:1_048_576L
                        in
                        match body with
                        | Error error -> return_error error
                        | Ok body -> (
                            match complete_result response body with
                            | Error error -> return_error error
                            | Ok result -> return_ok result)))))

  let abort conn ~bucket ~key ~upload_id =
    match validate_bucket_key bucket key with
    | Error error -> return_error error
    | Ok () -> (
        match object_request conn ~bucket ~key with
        | Error error -> return_error error
        | Ok request ->
            with_empty_response conn ~method_:`DELETE ~request
              ~query:
                [
                  ( "uploadId",
                    [ Public_multipart.Upload_id.to_string upload_id ] );
                ]
              ~headers:[]
              ~f:(fun response body ->
                let* discarded = discard_download_body body in
                match discarded with
                | Error error -> return_error error
                | Ok () -> return_ok response))

  let validate_list_parts_options
      (options : Public_multipart.List_parts.options) =
    match options.max_parts with
    | Some value when value <= 0 ->
        invalid ~field:"max_parts" "max_parts must be greater than zero"
    | _ -> (
        match options.part_number_marker with
        | Some value when value < 0 ->
            invalid ~field:"part_number_marker"
              "part_number_marker must be non-negative"
        | _ -> Ok ())

  let list_parts conn ~bucket ~key ~upload_id ?options () =
    let options =
      Option.value ~default:Public_multipart.List_parts.default_options options
    in
    match validate_bucket_key bucket key with
    | Error error -> return_error error
    | Ok () -> (
        match validate_list_parts_options options with
        | Error error -> return_error error
        | Ok () -> (
            match object_request conn ~bucket ~key with
            | Error error -> return_error error
            | Ok request ->
                let add name = function
                  | None -> []
                  | Some value -> [ (name, [ string_of_int value ]) ]
                in
                let query =
                  [
                    ( "uploadId",
                      [ Public_multipart.Upload_id.to_string upload_id ] );
                  ]
                  @ add "max-parts" options.max_parts
                  @ add "part-number-marker" options.part_number_marker
                in
                with_empty_response conn ~method_:`GET ~request ~query
                  ~headers:[] ~f:(fun response body ->
                    let* body = read_download_body body ~max_size:1_048_576L in
                    match body with
                    | Error error -> return_error error
                    | Ok body -> (
                        match Xml.decode_root body ~name:"ListPartsResult" with
                        | Error error -> return_error error
                        | Ok nodes ->
                            let parts =
                              Xml.children "Part" nodes
                              |> List.filter_map (fun nodes ->
                                  match
                                    Option.bind
                                      (Xml.child_text "PartNumber" nodes)
                                      int_of_string_opt
                                  with
                                  | None -> None
                                  | Some part_number ->
                                      Some
                                        {
                                          Public_multipart.List_parts
                                          .part_number;
                                          etag =
                                            Option.bind
                                              (Xml.child_text "ETag" nodes)
                                              (fun v ->
                                                Result.to_option
                                                  (Public_object.Etag.of_string
                                                     v));
                                          size =
                                            Option.bind
                                              (Xml.child_text "Size" nodes)
                                              int64_of_string_opt;
                                          last_modified =
                                            Option.bind
                                              (Xml.child_text "LastModified"
                                                 nodes)
                                              ptime_of_string;
                                          checksum = None;
                                        })
                            in
                            return_ok
                              {
                                Public_multipart.List_parts.parts;
                                is_truncated =
                                  Option.value ~default:false
                                    (Option.bind
                                       (Xml.child_text "IsTruncated" nodes)
                                       parse_bool);
                                next_part_number_marker =
                                  Option.bind
                                    (Xml.child_text "NextPartNumberMarker" nodes)
                                    int_of_string_opt;
                                request = response;
                              }))))

  module Paginator = struct
    let validate_max_pages = function
      | None -> Ok ()
      | Some value when value > 0 -> Ok ()
      | Some _ ->
          invalid ~field:"max_pages" "max_pages must be greater than zero"

    let options_for_page (base : Public_multipart.List_parts.options)
        part_number_marker =
      { base with Public_multipart.List_parts.part_number_marker }

    let fold_pages conn ~bucket ~key ~upload_id ?options ?max_pages ~init ~f ()
        =
      match validate_max_pages max_pages with
      | Error error -> return_error error
      | Ok () ->
          let base =
            Option.value ~default:Public_multipart.List_parts.default_options
              options
          in
          let rec loop part_number_marker page_count acc =
            let options = options_for_page base part_number_marker in
            let* page = list_parts conn ~bucket ~key ~upload_id ~options () in
            match page with
            | Error error -> return_error error
            | Ok page -> (
                let* next_acc = f acc page in
                match next_acc with
                | Error error -> return_error error
                | Ok acc -> (
                    let page_count = page_count + 1 in
                    if not page.is_truncated then return_ok acc
                    else
                      match max_pages with
                      | Some max_pages when page_count >= max_pages ->
                          return_ok acc
                      | _ -> (
                          match page.next_part_number_marker with
                          | Some marker -> loop (Some marker) page_count acc
                          | None ->
                              return_error
                                (decode
                                   "truncated list-parts response missing \
                                    NextPartNumberMarker"))))
          in
          loop base.part_number_marker 0 init

    let pages conn ~bucket ~key ~upload_id ?options ?max_pages () =
      let f pages page = return_ok (page :: pages) in
      let* result =
        fold_pages conn ~bucket ~key ~upload_id ?options ?max_pages ~init:[] ~f
          ()
      in
      return (Result.map List.rev result)

    let parts conn ~bucket ~key ~upload_id ?options ?max_pages () =
      let f parts (page : Public_multipart.List_parts.page) =
        return_ok (List.rev_append page.parts parts)
      in
      let* result =
        fold_pages conn ~bucket ~key ~upload_id ?options ?max_pages ~init:[] ~f
          ()
      in
      return (Result.map List.rev result)
  end

  module Managed = struct
    let ensure_part_count ~part_size ~length =
      if length = 0 then
        Error
          (Awskit.Error.validation ~field:"body"
             "managed multipart upload requires a non-empty body")
      else
        let count = (length + part_size - 1) / part_size in
        if count > Public_multipart.Managed.max_parts then
          Error
            (Awskit.Error.validation ~field:"part_count"
               "managed multipart upload would exceed 10000 parts")
        else Ok count

    let upload_string conn ~bucket ~key ?options body =
      let options =
        Option.value ~default:Public_multipart.Managed.default_options options
      in
      match Public_multipart.Managed.validate_options options with
      | Error error -> return_error error
      | Ok () -> (
          match
            ensure_part_count ~part_size:options.part_size
              ~length:(String.length body)
          with
          | Error error -> return_error error
          | Ok _ -> (
              let* created =
                create conn ~bucket ~key ~options:options.create_options ()
              in
              match created with
              | Error error -> return_error error
              | Ok created -> (
                  let upload_id = created.upload.upload_id in
                  let abort_and_return error =
                    let* _ = abort conn ~bucket ~key ~upload_id in
                    return_error error
                  in
                  let rec upload_parts offset part_number parts =
                    if offset >= String.length body then
                      return_ok (List.rev parts)
                    else
                      let length =
                        min options.part_size (String.length body - offset)
                      in
                      let part_body = String.sub body offset length in
                      let* uploaded =
                        upload_part conn ~bucket ~key ~upload_id ~part_number
                          ~body:(R.string_body part_body)
                          ~options:options.upload_part_options ()
                      in
                      match uploaded with
                      | Error error -> abort_and_return error
                      | Ok uploaded ->
                          upload_parts (offset + length) (part_number + 1)
                            (uploaded.part :: parts)
                  in
                  let* parts = upload_parts 0 1 [] in
                  match parts with
                  | Error error -> return_error error
                  | Ok parts -> (
                      let* completed =
                        complete conn ~bucket ~key ~upload_id parts
                      in
                      match completed with
                      | Error error -> abort_and_return error
                      | Ok complete ->
                          return_ok
                            {
                              Public_multipart.Managed.upload = created.upload;
                              parts;
                              complete;
                            }))))

    let upload_bytes conn ~bucket ~key ?options body =
      upload_string conn ~bucket ~key ?options (Bytes.to_string body)
  end
end
