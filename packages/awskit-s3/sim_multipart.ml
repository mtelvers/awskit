open Core
open Sim_support
module Public_multipart = Multipart
module Public_object = Object

module Multipart = struct
  type connection = t
  type 'a io = 'a
  type upload_body = Runtime.upload_body

  let validate_create_options (options : Public_multipart.Create.options) =
    let* () = validate_metadata options.metadata in
    validate_tags options.tags

  let create conn ~bucket ~key ?options () =
    let options =
      Option.value ~default:Public_multipart.Create.default_options options
    in
    match validate_bucket_key bucket key with
    | Error error -> Error error
    | Ok () -> (
        match require_bucket conn bucket with
        | Error error -> Error error
        | Ok bucket_state -> (
            match validate_create_options options with
            | Error error -> Error error
            | Ok () -> (
                match
                  operation_fault conn `Multipart_create bucket (Some key)
                with
                | Some error -> Error error
                | None ->
                    let upload_id = next_upload_id conn in
                    let upload =
                      { Public_multipart.Upload.bucket; key; upload_id }
                    in
                    Hashtbl.replace bucket_state.multipart_uploads
                      (upload_key upload_id)
                      {
                        upload;
                        content_type = options.content_type;
                        metadata = options.metadata;
                        storage_class = options.storage_class;
                        tags = options.tags;
                        checksum_request = options.checksum;
                        parts = Hashtbl.create 17;
                        created_at = now conn;
                      };
                    Ok
                      { Public_multipart.Create.upload; request = response 200 }
                )))

  let upload_part conn ~bucket ~key ~upload_id ~part_number ~body ?options () =
    let options =
      Option.value ~default:Public_multipart.Upload_part.default_options options
    in
    match validate_bucket_key bucket key with
    | Error error -> Error error
    | Ok () -> (
        match Public_multipart.Part.create ~part_number ~etag:"unused" with
        | Error error -> Error error
        | Ok _ -> (
            match require_multipart_upload conn ~bucket ~key ~upload_id with
            | Error error -> Error error
            | Ok (_bucket_state, upload) -> (
                match
                  operation_fault conn `Multipart_upload_part bucket (Some key)
                with
                | Some error -> Error error
                | None -> (
                    match Runtime.upload_body_result body with
                    | Error error -> Error error
                    | Ok body ->
                        let etag = etag body in
                        let checksum =
                          checksum_for_body ~body options.checksum
                        in
                        let part =
                          {
                            part_number;
                            body;
                            etag;
                            checksum;
                            last_modified = now conn;
                          }
                        in
                        Hashtbl.replace upload.parts part_number part;
                        let part =
                          Public_multipart.Part.create_exn ~part_number ~etag
                        in
                        Ok
                          {
                            Public_multipart.Upload_part.part;
                            checksum;
                            request =
                              response 200
                                ~headers:
                                  (("etag", etag)
                                  :: checksum_response_headers checksum);
                          }))))

  let validate_complete_parts upload parts =
    let rec loop previous = function
      | [] -> Ok ()
      | (part : Public_multipart.Part.t) :: rest -> (
          match previous with
          | Some previous when part.part_number <= previous ->
              invalid ~field:"part_number" "parts must be sorted by part_number"
          | _ -> (
              match Hashtbl.find_opt upload.parts part.part_number with
              | None ->
                  Error
                    (service ~status:400 ~code:"InvalidPart"
                       ~message:"multipart part is missing" ())
              | Some stored
                when not (Public_object.Etag.equal stored.etag part.etag) ->
                  Error
                    (service ~status:400 ~code:"InvalidPart"
                       ~message:"multipart part etag does not match" ())
              | Some _ -> loop (Some part.part_number) rest))
    in
    match parts with
    | [] ->
        Error
          (Awskit.Error.validation ~field:"parts"
             "complete requires at least one part")
    | parts -> loop None parts

  let complete conn ~bucket ~key ~upload_id parts =
    match validate_bucket_key bucket key with
    | Error error -> Error error
    | Ok () -> (
        match require_multipart_upload conn ~bucket ~key ~upload_id with
        | Error error -> Error error
        | Ok (bucket_state, upload) -> (
            match validate_complete_parts upload parts with
            | Error error -> Error error
            | Ok () -> (
                match
                  operation_fault conn `Multipart_complete bucket (Some key)
                with
                | Some error -> Error error
                | None ->
                    let body =
                      parts
                      |> List.map (fun (part : Public_multipart.Part.t) ->
                          (Hashtbl.find upload.parts part.part_number).body)
                      |> String.concat ""
                    in
                    let etag = etag body in
                    let checksum =
                      checksum_for_body ~body upload.checksum_request
                    in
                    let obj =
                      {
                        body;
                        etag;
                        version_id = None;
                        content_type = upload.content_type;
                        metadata = upload.metadata;
                        storage_class = upload.storage_class;
                        tags = upload.tags;
                        checksum;
                        last_modified = now conn;
                      }
                    in
                    let obj = store_object conn bucket_state key obj in
                    Hashtbl.remove bucket_state.multipart_uploads
                      (upload_key upload_id);
                    Ok
                      {
                        Public_multipart.Complete.etag = Some etag;
                        version_id = obj.version_id;
                        checksum;
                        request =
                          response 200
                            ~headers:
                              (version_headers obj.version_id
                              @ checksum_response_headers checksum);
                      })))

  let abort conn ~bucket ~key ~upload_id =
    match validate_bucket_key bucket key with
    | Error error -> Error error
    | Ok () -> (
        match require_multipart_upload conn ~bucket ~key ~upload_id with
        | Error error -> Error error
        | Ok (bucket_state, _upload) -> (
            match operation_fault conn `Multipart_abort bucket (Some key) with
            | Some error -> Error error
            | None ->
                Hashtbl.remove bucket_state.multipart_uploads
                  (upload_key upload_id);
                Ok (response 204)))

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
    | Error error -> Error error
    | Ok () -> (
        match validate_list_parts_options options with
        | Error error -> Error error
        | Ok () -> (
            match require_multipart_upload conn ~bucket ~key ~upload_id with
            | Error error -> Error error
            | Ok (_bucket_state, upload) -> (
                match
                  operation_fault conn `Multipart_list_parts bucket (Some key)
                with
                | Some error -> Error error
                | None ->
                    let max_parts =
                      Option.value ~default:conn.store.config.max_list_keys
                        options.max_parts
                    in
                    let all =
                      Hashtbl.to_seq_values upload.parts
                      |> List.of_seq
                      |> List.sort (fun a b ->
                          Int.compare a.part_number b.part_number)
                      |> List.filter (fun part ->
                          match options.part_number_marker with
                          | None -> true
                          | Some marker -> part.part_number > marker)
                    in
                    let selected =
                      all |> List.to_seq |> Seq.take max_parts |> List.of_seq
                    in
                    let is_truncated = List.length all > List.length selected in
                    let next_part_number_marker =
                      if not is_truncated then None
                      else
                        match List.rev selected with
                        | [] -> None
                        | part :: _ -> Some part.part_number
                    in
                    let parts =
                      List.map
                        (fun part ->
                          {
                            Public_multipart.List_parts.part_number =
                              part.part_number;
                            etag = Some part.etag;
                            size = Some (Int64.of_int (String.length part.body));
                            last_modified = Some part.last_modified;
                            checksum = part.checksum;
                          })
                        selected
                    in
                    Ok
                      {
                        Public_multipart.List_parts.parts;
                        is_truncated;
                        next_part_number_marker;
                        request = response 200;
                      })))

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
      | Error error -> Error error
      | Ok () ->
          let base =
            Option.value ~default:Public_multipart.List_parts.default_options
              options
          in
          let rec loop part_number_marker page_count acc =
            let options = options_for_page base part_number_marker in
            match list_parts conn ~bucket ~key ~upload_id ~options () with
            | Error error -> Error error
            | Ok page -> (
                match f acc page with
                | Error error -> Error error
                | Ok acc -> (
                    let page_count = page_count + 1 in
                    if not page.is_truncated then Ok acc
                    else
                      match max_pages with
                      | Some max_pages when page_count >= max_pages -> Ok acc
                      | _ -> (
                          match page.next_part_number_marker with
                          | Some marker -> loop (Some marker) page_count acc
                          | None ->
                              Error
                                (decode
                                   "truncated list-parts response missing \
                                    NextPartNumberMarker"))))
          in
          loop base.part_number_marker 0 init

    let pages conn ~bucket ~key ~upload_id ?options ?max_pages () =
      fold_pages conn ~bucket ~key ~upload_id ?options ?max_pages ~init:[]
        ~f:(fun pages page -> Ok (page :: pages))
        ()
      |> Result.map List.rev

    let parts conn ~bucket ~key ~upload_id ?options ?max_pages () =
      fold_pages conn ~bucket ~key ~upload_id ?options ?max_pages ~init:[]
        ~f:(fun parts (page : Public_multipart.List_parts.page) ->
          Ok (List.rev_append page.parts parts))
        ()
      |> Result.map List.rev
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
      | Error error -> Error error
      | Ok () -> (
          match
            ensure_part_count ~part_size:options.part_size
              ~length:(String.length body)
          with
          | Error error -> Error error
          | Ok _ -> (
              match
                create conn ~bucket ~key ~options:options.create_options ()
              with
              | Error error -> Error error
              | Ok created -> (
                  let upload_id = created.upload.upload_id in
                  let abort_and_return error =
                    ignore (abort conn ~bucket ~key ~upload_id);
                    Error error
                  in
                  let rec upload_parts offset part_number parts =
                    if offset >= String.length body then Ok (List.rev parts)
                    else
                      let length =
                        min options.part_size (String.length body - offset)
                      in
                      let part_body = String.sub body offset length in
                      match
                        upload_part conn ~bucket ~key ~upload_id ~part_number
                          ~body:(Runtime.string_body part_body)
                          ~options:options.upload_part_options ()
                      with
                      | Error error -> abort_and_return error
                      | Ok uploaded ->
                          upload_parts (offset + length) (part_number + 1)
                            (uploaded.part :: parts)
                  in
                  match upload_parts 0 1 [] with
                  | Error error -> Error error
                  | Ok parts -> (
                      match complete conn ~bucket ~key ~upload_id parts with
                      | Error error -> abort_and_return error
                      | Ok complete ->
                          Ok
                            {
                              Public_multipart.Managed.upload = created.upload;
                              parts;
                              complete;
                            }))))

    let upload_bytes conn ~bucket ~key ?options body =
      upload_string conn ~bucket ~key ?options (Bytes.to_string body)
  end
end
