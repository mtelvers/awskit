open Core

module Make (C : Operation_context.S) = struct
  open C

  let ( let* ) = bind

  type nonrec connection = connection
  type 'a io = 'a R.t
  type nonrec upload_body = upload_body
  type nonrec download_reader = download_reader

  let validate_put_options (options : Object.Put.options) =
    match validate_metadata options.metadata with
    | Error _ as error -> error
    | Ok () -> (
        match validate_tags options.tags with
        | Error _ as error -> error
        | Ok () ->
            validate_common_headers ?content_type:options.content_type
              ?cache_control:options.cache_control
              ?content_encoding:options.content_encoding
              ?content_disposition:options.content_disposition ())

  let put conn ~bucket ~key ?options ~body () =
    let options = Option.value ~default:Object.Put.default_options options in
    match validate_bucket_key bucket key with
    | Error error -> return_error error
    | Ok () -> (
        match validate_put_options options with
        | Error error -> return_error error
        | Ok () -> (
            let descriptor = R.upload_descriptor body in
            match descriptor.content_length with
            | None ->
                return_error
                  (Awskit.Error.validation ~field:"content_length"
                     "S3 uploads require a known content length before SigV4 \
                      chunked streaming")
            | Some content_length -> (
                match Awskit.Body.Upload.validate_descriptor descriptor with
                | Error error -> return_error error
                | Ok () -> (
                    let headers =
                      [ ("content-length", Int64.to_string content_length) ]
                      @ Metadata_headers.to_headers options.metadata
                      @ write_precondition_headers options.preconditions
                      @ checksum_request_headers options.checksum
                      @ encryption_request_headers
                          options.server_side_encryption
                      |> add_opt_header "content-type" options.content_type
                      |> add_opt_header "cache-control" options.cache_control
                      |> add_opt_header "content-encoding"
                           options.content_encoding
                      |> add_opt_header "content-disposition"
                           options.content_disposition
                      |> add_opt_header "x-amz-storage-class"
                           (Option.map Storage_class.to_string
                              options.storage_class)
                      |> add_opt_header "x-amz-tagging"
                           (tags_header options.tags)
                    in
                    match object_request conn ~bucket ~key with
                    | Error error -> return_error error
                    | Ok request ->
                        with_response conn ~method_:`PUT ~request ~query:[]
                          ~headers ~payload_hash:descriptor.payload_hash body
                          ~f:(fun response body ->
                            let* discarded = discard_download_body body in
                            match discarded with
                            | Error error -> return_error error
                            | Ok () -> return (put_result response))))))

  let get conn ~bucket ~key ?options ~consume () =
    let options = Option.value ~default:Object.Get.default_options options in
    match validate_bucket_key bucket key with
    | Error error -> return_error error
    | Ok () -> (
        let headers =
          read_precondition_headers options.preconditions
          |> add_opt_header "range" (Option.map Range.to_header options.range)
        in
        let query =
          match options.version_id with
          | None -> []
          | Some version_id ->
              [ ("versionId", [ Object.Version_id.to_string version_id ]) ]
        in
        match object_request conn ~bucket ~key with
        | Error error -> return_error error
        | Ok request ->
            with_empty_response conn ~method_:`GET ~request ~query ~headers
              ~f:(fun response body ->
                match object_info response with
                | Error error -> return_error error
                | Ok info ->
                    let* consumed = R.with_download_body body ~consume in
                    return (Result.map (fun value -> (info, value)) consumed)))

  let head conn ~bucket ~key ?options () =
    let options = Option.value ~default:Object.Head.default_options options in
    match validate_bucket_key bucket key with
    | Error error -> return_error error
    | Ok () -> (
        let headers = read_precondition_headers options.preconditions in
        let query =
          match options.version_id with
          | None -> []
          | Some version_id ->
              [ ("versionId", [ Object.Version_id.to_string version_id ]) ]
        in
        match object_request conn ~bucket ~key with
        | Error error -> return_error error
        | Ok request ->
            with_empty_response conn ~method_:`HEAD ~request ~query ~headers
              ~f:(fun response body ->
                let* discarded = discard_download_body body in
                match discarded with
                | Error error -> return_error error
                | Ok () -> return (object_info response)))

  let exists conn ~bucket ~key =
    let* result = head conn ~bucket ~key () in
    match result with
    | Ok _ -> return_ok true
    | Error error when Error.is_not_found error -> return_ok false
    | Error error -> return_error error

  let delete conn ~bucket ~key ?options () =
    let options = Option.value ~default:Object.Delete.default_options options in
    match validate_bucket_key bucket key with
    | Error error -> return_error error
    | Ok () -> (
        let headers = delete_precondition_headers options.preconditions in
        let query =
          match options.version_id with
          | None -> []
          | Some version_id ->
              [ ("versionId", [ Object.Version_id.to_string version_id ]) ]
        in
        match object_request conn ~bucket ~key with
        | Error error -> return_error error
        | Ok request ->
            with_empty_response conn ~method_:`DELETE ~request ~query ~headers
              ~f:(fun response body ->
                let* discarded = discard_download_body body in
                match discarded with
                | Error error -> return_error error
                | Ok () -> return (delete_result response)))

  let delete_many conn ~bucket ~objects =
    match validate_bucket bucket with
    | Error error -> return_error error
    | Ok () -> (
        match Object_delete_xml.validate_objects objects with
        | Error error -> return_error error
        | Ok () -> (
            let body = Object_delete_xml.body objects in
            let headers =
              [
                ("content-md5", content_md5 body);
                ("content-type", "application/xml");
              ]
            in
            let upload = R.string_body body in
            match
              bucket_request conn ~bucket ~suffix:"/" ~signing_suffix:"/"
            with
            | Error error -> return_error error
            | Ok request ->
                with_response conn ~method_:`POST ~request
                  ~query:[ ("delete", []) ]
                  ~headers
                  ~payload_hash:(R.upload_descriptor upload).payload_hash upload
                  ~f:(fun response response_body ->
                    let* body =
                      read_download_body response_body ~max_size:1_048_576L
                    in
                    match body with
                    | Error error -> return_error error
                    | Ok body ->
                        return
                          (Object_delete_xml.parse_result ~request:response body))
            ))

  let copy conn ~src_bucket ~src_key ~dst_bucket ~dst_key ?options () =
    let options = Option.value ~default:Object.Copy.default_options options in
    match validate_bucket_key src_bucket src_key with
    | Error error -> return_error error
    | Ok () -> (
        match validate_bucket_key dst_bucket dst_key with
        | Error error -> return_error error
        | Ok () -> (
            let copy_source =
              Awskit.Signing.uri_encode ~encode_slash:false
                (Fmt.str "/%s/%s" src_bucket src_key)
            in
            let copy_source =
              match options.source_version_id with
              | None -> copy_source
              | Some version_id ->
                  Fmt.str "%s?versionId=%s" copy_source
                    (Awskit.Signing.uri_encode ~encode_slash:true
                       (Object.Version_id.to_string version_id))
            in
            let headers =
              ("x-amz-copy-source", copy_source)
              :: copy_source_precondition_headers options.source_preconditions
              @ checksum_request_headers options.checksum
              @ encryption_request_headers options.server_side_encryption
            in
            let headers =
              match options.metadata with
              | None -> headers
              | Some `Copy -> ("x-amz-metadata-directive", "COPY") :: headers
              | Some (`Replace metadata) ->
                  ("x-amz-metadata-directive", "REPLACE")
                  :: Metadata_headers.to_headers metadata
                  @ headers
            in
            let headers =
              headers
              |> add_opt_header "x-amz-storage-class"
                   (Option.map Storage_class.to_string options.storage_class)
              |> add_opt_header "x-amz-tagging"
                   (Option.bind options.tags tags_header)
            in
            match object_request conn ~bucket:dst_bucket ~key:dst_key with
            | Error error -> return_error error
            | Ok request ->
                with_empty_response conn ~method_:`PUT ~request ~query:[]
                  ~headers ~f:(fun response body ->
                    let* body = read_download_body body ~max_size:1_048_576L in
                    match body with
                    | Error error -> return_error error
                    | Ok body -> return (copy_result response body))))

  let list_versions conn ~bucket ?options () =
    let options =
      Option.value ~default:Object.Versions.default_options options
    in
    match validate_bucket bucket with
    | Error error -> return_error error
    | Ok () -> (
        let add name = function
          | None -> []
          | Some value -> [ (name, [ value ]) ]
        in
        let query =
          [ ("versions", []) ]
          @ add "prefix" options.prefix
          @ add "delimiter" options.delimiter
          @ add "max-keys" (Option.map string_of_int options.max_keys)
          @ add "key-marker" options.key_marker
          @ add "version-id-marker"
              (Option.map Object.Version_id.to_string options.version_id_marker)
        in
        match bucket_request conn ~bucket ~suffix:"/" ~signing_suffix:"/" with
        | Error error -> return_error error
        | Ok request ->
            with_empty_response conn ~method_:`GET ~request ~query ~headers:[]
              ~f:(fun response body ->
                let* body = read_download_body body ~max_size:4_194_304L in
                match body with
                | Error error -> return_error error
                | Ok body ->
                    return
                      (Object_versions_xml.parse_page ~request:response body)))

  let list conn ~bucket ?options () =
    let options = Option.value ~default:Object.List.default_options options in
    match validate_bucket bucket with
    | Error error -> return_error error
    | Ok () -> (
        let add name = function
          | None -> []
          | Some value -> [ (name, [ value ]) ]
        in
        let query =
          [ ("list-type", [ "2" ]) ]
          @ add "prefix" options.prefix
          @ add "delimiter" options.delimiter
          @ add "max-keys" (Option.map string_of_int options.max_keys)
          @ add "start-after" options.start_after
          @ add "continuation-token" options.continuation_token
        in
        match bucket_request conn ~bucket ~suffix:"/" ~signing_suffix:"/" with
        | Error error -> return_error error
        | Ok request ->
            with_empty_response conn ~method_:`GET ~request ~query ~headers:[]
              ~f:(fun response body ->
                let* body = read_download_body body ~max_size:4_194_304L in
                match body with
                | Error error -> return_error error
                | Ok body ->
                    return (Object_list_xml.parse_page ~request:response body)))

  let list_keys conn ~bucket ?options () =
    let* page = list conn ~bucket ?options () in
    return
      (Result.map
         (fun (page : Object.List.page) ->
           List.map (fun (o : Object.List.object_summary) -> o.key) page.objects)
         page)

  module Paginator = struct
    let validate_max_pages = function
      | None -> Ok ()
      | Some value when value > 0 -> Ok ()
      | Some _ ->
          invalid ~field:"max_pages" "max_pages must be greater than zero"

    let options_for_page (base : Object.List.options) continuation_token =
      {
        base with
        Object.List.continuation_token;
        start_after =
          (match continuation_token with
          | None -> base.start_after
          | Some _ -> None);
      }

    let fold_pages conn ~bucket ?options ?max_pages ~init ~f () =
      match validate_max_pages max_pages with
      | Error error -> return_error error
      | Ok () ->
          let base =
            Option.value ~default:Object.List.default_options options
          in
          let rec loop continuation_token page_count acc =
            let options = options_for_page base continuation_token in
            let* page = list conn ~bucket ~options () in
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
                          match page.next_continuation_token with
                          | Some token -> loop (Some token) page_count acc
                          | None ->
                              return_error
                                (decode
                                   "truncated list response missing \
                                    NextContinuationToken"))))
          in
          loop base.continuation_token 0 init

    let pages conn ~bucket ?options ?max_pages () =
      let f pages page = return_ok (page :: pages) in
      let* result =
        fold_pages conn ~bucket ?options ?max_pages ~init:[] ~f ()
      in
      return (Result.map List.rev result)

    let objects conn ~bucket ?options ?max_pages () =
      let f objects (page : Object.List.page) =
        return_ok (List.rev_append page.objects objects)
      in
      let* result =
        fold_pages conn ~bucket ?options ?max_pages ~init:[] ~f ()
      in
      return (Result.map List.rev result)

    let keys conn ~bucket ?options ?max_pages () =
      let f keys (page : Object.List.page) =
        let page_keys =
          List.map
            (fun (object_ : Object.List.object_summary) -> object_.key)
            page.objects
        in
        return_ok (List.rev_append page_keys keys)
      in
      let* result =
        fold_pages conn ~bucket ?options ?max_pages ~init:[] ~f ()
      in
      return (Result.map List.rev result)
  end

  module Versions = struct
    let validate_max_pages = function
      | None -> Ok ()
      | Some value when value > 0 -> Ok ()
      | Some _ ->
          invalid ~field:"max_pages" "max_pages must be greater than zero"

    let options_for_page (base : Object.Versions.options) page =
      {
        base with
        Object.Versions.key_marker = page.Object.Versions.next_key_marker;
        version_id_marker = page.next_version_id_marker;
      }

    let fold_pages conn ~bucket ?options ?max_pages ~init ~f () =
      match validate_max_pages max_pages with
      | Error error -> return_error error
      | Ok () ->
          let base =
            Option.value ~default:Object.Versions.default_options options
          in
          let rec loop options page_count acc =
            let* page = list_versions conn ~bucket ~options () in
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
                          match page.next_key_marker with
                          | Some _ ->
                              loop (options_for_page base page) page_count acc
                          | None ->
                              return_error
                                (decode
                                   "truncated version listing response missing \
                                    NextKeyMarker"))))
          in
          loop base 0 init

    let pages conn ~bucket ?options ?max_pages () =
      let f pages page = return_ok (page :: pages) in
      let* result =
        fold_pages conn ~bucket ?options ?max_pages ~init:[] ~f ()
      in
      return (Result.map List.rev result)

    let object_versions conn ~bucket ?options ?max_pages () =
      let f versions (page : Object.Versions.page) =
        return_ok (List.rev_append page.versions versions)
      in
      let* result =
        fold_pages conn ~bucket ?options ?max_pages ~init:[] ~f ()
      in
      return (Result.map List.rev result)

    let delete_markers conn ~bucket ?options ?max_pages () =
      let f markers (page : Object.Versions.page) =
        return_ok (List.rev_append page.delete_markers markers)
      in
      let* result =
        fold_pages conn ~bucket ?options ?max_pages ~init:[] ~f ()
      in
      return (Result.map List.rev result)
  end

  module Buffer = struct
    let put_string conn ~bucket ~key ?options body =
      put conn ~bucket ~key ?options ~body:(R.string_body body) ()

    let put_bytes conn ~bucket ~key ?options body =
      put conn ~bucket ~key ?options ~body:(R.bytes_body body) ()

    let get_string conn ~bucket ~key ~max_size ?options () =
      let consume reader = read_body reader ~max_size in
      get conn ~bucket ~key ?options ~consume ()

    let get_bytes conn ~bucket ~key ~max_size ?options () =
      let* result = get_string conn ~bucket ~key ~max_size ?options () in
      return
        (Result.map (fun (info, body) -> (info, Bytes.of_string body)) result)
  end

  module Tagging = struct
    let get conn ~bucket ~key =
      match validate_bucket_key bucket key with
      | Error error -> return_error error
      | Ok () -> (
          match object_request conn ~bucket ~key with
          | Error error -> return_error error
          | Ok request ->
              with_empty_response conn ~method_:`GET ~request
                ~query:[ ("tagging", []) ]
                ~headers:[]
                ~f:(fun response body ->
                  let* body = read_download_body body ~max_size:1_048_576L in
                  match body with
                  | Error error -> return_error error
                  | Ok body ->
                      return
                        (Result.map
                           (fun tags ->
                             { Object.Tagging.tags; request = response })
                           (parse_tags body))))

    let put conn ~bucket ~key tags =
      match validate_bucket_key bucket key with
      | Error error -> return_error error
      | Ok () -> (
          match validate_tags tags with
          | Error error -> return_error error
          | Ok () -> (
              let body = xml_tags tags in
              let upload = R.string_body body in
              let headers =
                [
                  ("content-md5", content_md5 body);
                  ("content-type", "application/xml");
                ]
              in
              match object_request conn ~bucket ~key with
              | Error error -> return_error error
              | Ok request ->
                  with_response conn ~method_:`PUT ~request
                    ~query:[ ("tagging", []) ]
                    ~headers
                    ~payload_hash:(R.upload_descriptor upload).payload_hash
                    upload
                    ~f:(fun response body ->
                      let* discarded = discard_download_body body in
                      match discarded with
                      | Error error -> return_error error
                      | Ok () -> return_ok response)))

    let delete conn ~bucket ~key =
      match validate_bucket_key bucket key with
      | Error error -> return_error error
      | Ok () -> (
          match object_request conn ~bucket ~key with
          | Error error -> return_error error
          | Ok request ->
              with_empty_response conn ~method_:`DELETE ~request
                ~query:[ ("tagging", []) ]
                ~headers:[]
                ~f:(fun response body ->
                  let* discarded = discard_download_body body in
                  match discarded with
                  | Error error -> return_error error
                  | Ok () -> return_ok response))
  end
end
