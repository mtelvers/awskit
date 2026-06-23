module Make
    (Runtime : Awskit_s3.RUNTIME with type 'a t = 'a)
    (S3 : sig
      module Object : sig
        val put :
          Runtime.connection ->
          bucket:string ->
          key:string ->
          ?options:Awskit_s3.Put_object.options ->
          body:Runtime.request_body ->
          unit ->
          (Awskit_s3.Put_object.result, Awskit_s3.Error.t) result

        val get :
          Runtime.connection ->
          bucket:string ->
          key:string ->
          ?options:Awskit_s3.Get_object.options ->
          consume:
            (Runtime.response_body_reader -> ('a, Awskit_s3.Error.t) result) ->
          unit ->
          (Awskit_s3.Get_object.result * 'a, Awskit_s3.Error.t) result

        val head :
          Runtime.connection ->
          bucket:string ->
          key:string ->
          ?options:Awskit_s3.Head_object.options ->
          unit ->
          (Awskit_s3.Head_object.result, Awskit_s3.Error.t) result
      end

      module Multipart :
        Awskit_s3.MULTIPART
          with type connection := Runtime.connection
           and type 'a io := 'a
           and type request_body := Runtime.request_body
    end) =
struct
  let buffer_size = 128 * 1024

  type part_spec = { part_number : int; offset : int64; length : int }
  type range_spec = { index : int; offset : int64; length : int }

  let ( let* ) result f =
    match result with Ok value -> f value | Error _ as error -> error

  let body_error action path exn =
    Awskit.Error.body
      (Fmt.str "failed to %s path %a: %s" action Eio.Path.pp path
         (Printexc.to_string exn))

  let regular_file_length path =
    try
      let stat = Eio.Path.stat ~follow:true path in
      match stat.kind with
      | `Regular_file -> Ok (Optint.Int63.to_int64 stat.size)
      | kind ->
          Error
            (Awskit.Error.validation ~field:"path"
               (Fmt.str "expected regular file, got %a" Eio.File.Stat.pp_kind
                  kind))
    with exn -> Error (body_error "stat upload" path exn)

  let request_body_of_path ?on_progress path =
    match regular_file_length path with
    | Error _ as error -> error
    | Ok content_length ->
        let descriptor =
          {
            Awskit.Body.Request.content_length = Some content_length;
            payload_hash = Awskit.Body.Payload_hash.unsigned_payload;
            replayable = true;
          }
        in
        let write writer =
          try
            Eio.Path.with_open_in path (fun file ->
                (* single_read fills the cstruct; read each chunk back out of
                   it. *)
                let cstruct = Cstruct.create buffer_size in
                let rec loop transferred =
                  match Eio.Flow.single_read file cstruct with
                  | n -> (
                      let chunk = Cstruct.to_string ~off:0 ~len:n cstruct in
                      match Runtime.Request_body.write_string writer chunk with
                      | Error _ as error -> error
                      | Ok () ->
                          let transferred =
                            Int64.add transferred (Int64.of_int n)
                          in
                          Option.iter (fun f -> f transferred) on_progress;
                          loop transferred)
                  | exception End_of_file -> Ok ()
                in
                loop 0L)
          with exn -> Error (body_error "read upload" path exn)
        in
        Ok (Runtime.Request_body.of_stream descriptor ~write)

  let put_file conn ~bucket ~key ?options ?on_progress ~path () =
    match request_body_of_path ?on_progress path with
    | Error _ as error -> error
    | Ok body -> S3.Object.put conn ~bucket ~key ?options ~body ()

  let bounded_specs ~content_length ~part_size ~empty_error ~make =
    if Int64.equal content_length 0L then
      match empty_error with None -> Ok [] | Some error -> Error error
    else
      let rec loop index offset acc =
        if Int64.compare offset content_length >= 0 then Ok (List.rev acc)
        else
          let remaining = Int64.sub content_length offset in
          let length = min part_size (Int64.to_int remaining) in
          loop (index + 1)
            (Int64.add offset (Int64.of_int length))
            (make index offset length :: acc)
      in
      loop 1 0L []

  let multipart_specs ~content_length ~part_size =
    let empty_error =
      Awskit.Error.validation ~field:"path"
        "multipart file upload requires a non-empty file"
    in
    let* () =
      Awskit_s3.Transfer.validate_multipart_part_count ~content_length
        ~part_size
    in
    bounded_specs ~content_length ~part_size ~empty_error:(Some empty_error)
      ~make:(fun part_number offset length -> { part_number; offset; length })

  let range_specs ~content_length ~part_size =
    let* () =
      Awskit_s3.Transfer.validate_multipart_part_count ~content_length
        ~part_size
    in
    bounded_specs ~content_length ~part_size ~empty_error:None
      ~make:(fun index offset length -> { index; offset; length })

  let range_body_of_path path (spec : part_spec) =
    let descriptor =
      {
        Awskit.Body.Request.content_length = Some (Int64.of_int spec.length);
        payload_hash = Awskit.Body.Payload_hash.unsigned_payload;
        replayable = true;
      }
    in
    let write writer =
      try
        Eio.Path.with_open_in path (fun file ->
            ignore (Eio.File.seek file (Optint.Int63.of_int64 spec.offset) `Set);
            let bytes = Bytes.create buffer_size in
            let rec loop remaining =
              if remaining = 0 then Ok ()
              else
                let len = min buffer_size remaining in
                let cstruct = Cstruct.of_bytes ~len bytes in
                match Eio.Flow.single_read file cstruct with
                | n -> (
                    let chunk = Bytes.sub_string bytes 0 n in
                    match Runtime.Request_body.write_string writer chunk with
                    | Error _ as error -> error
                    | Ok () -> loop (remaining - n))
                | exception End_of_file ->
                    Error
                      (Awskit.Error.body
                         (Fmt.str
                            "unexpected end of file while reading part %d from \
                             %a"
                            spec.part_number Eio.Path.pp path))
            in
            loop spec.length)
      with exn -> Error (body_error "read multipart upload" path exn)
    in
    Runtime.Request_body.of_stream descriptor ~write

  let split_batch ~concurrency specs =
    let rec loop remaining acc = function
      | rest when remaining = 0 -> (List.rev acc, rest)
      | [] -> (List.rev acc, [])
      | spec :: rest -> loop (remaining - 1) (spec :: acc) rest
    in
    loop concurrency [] specs

  let first_error_or_parts results =
    let rec loop acc = function
      | [] -> Ok (List.rev acc)
      | Error error :: _ -> Error error
      | Ok part :: rest -> loop (part :: acc) rest
    in
    loop [] results

  let first_error_or_unit results =
    let rec loop = function
      | [] -> Ok ()
      | Error error :: _ -> Error error
      | Ok () :: rest -> loop rest
    in
    loop results

  let upload_part_from_path conn ~bucket ~key ~upload_id ~options ~path
      (spec : part_spec) =
    let body = range_body_of_path path spec in
    match
      S3.Multipart.upload_part conn ~bucket ~key ~upload_id
        ~part_number:spec.part_number ~body
        ~options:options.Awskit_s3.Transfer.upload_part_options ()
    with
    | Error _ as error -> error
    | Ok uploaded -> Ok uploaded.part

  let upload_missing_parts conn ~bucket ~key ~upload_id ~options ~path
      ?on_progress ~initial_completed specs =
    let completed = ref initial_completed in
    Option.iter
      (fun f -> if Int64.compare !completed 0L > 0 then f !completed)
      on_progress;
    let upload_one spec =
      match
        upload_part_from_path conn ~bucket ~key ~upload_id ~options ~path spec
      with
      | Error _ as error -> error
      | Ok part ->
          completed := Int64.add !completed (Int64.of_int spec.length);
          Option.iter (fun f -> f !completed) on_progress;
          Ok part
    in
    let rec loop acc specs =
      match specs with
      | [] -> Ok (List.rev acc)
      | _ ->
          let batch, rest =
            split_batch ~concurrency:options.Awskit_s3.Transfer.concurrency
              specs
          in
          let results =
            Eio.Switch.run @@ fun sw ->
            batch
            |> List.map (fun spec ->
                Eio.Fiber.fork_promise ~sw (fun () -> upload_one spec))
            |> List.map Eio.Promise.await_exn
          in
          let* parts = first_error_or_parts results in
          loop (List.rev_append parts acc) rest
    in
    loop [] specs

  let sort_parts parts =
    List.sort
      (fun (left : Awskit_s3.Multipart.Part.t) right ->
        compare left.part_number right.part_number)
      parts

  let completed_bytes specs parts =
    parts
    |> List.fold_left
         (fun total (part : Awskit_s3.Multipart.Part.t) ->
           match
             List.find_opt
               (fun spec -> spec.part_number = part.part_number)
               specs
           with
           | None -> total
           | Some spec -> Int64.add total (Int64.of_int spec.length))
         0L

  let matching_uploaded_parts conn ~bucket ~key ~upload_id ~options specs =
    match
      S3.Multipart.List_parts.parts conn ~bucket ~key ~upload_id
        ~options:options.Awskit_s3.Transfer.list_parts_options ()
    with
    | Error _ as error -> error
    | Ok uploaded ->
        let part_for_spec spec =
          match
            List.find_opt
              (fun (part : Awskit_s3.List_parts.part_info) ->
                part.part_number = spec.part_number)
              uploaded
          with
          | None -> None
          | Some part -> (
              match (part.etag, part.size) with
              | Some etag, Some size
                when Int64.equal size (Int64.of_int spec.length) ->
                  Awskit_s3.Multipart.Part.create ~part_number:spec.part_number
                    ~etag ()
                  |> Result.to_option
              | _ -> None)
        in
        Ok (List.filter_map part_for_spec specs)

  let remaining_specs specs uploaded_parts =
    specs
    |> List.filter (fun spec ->
        not
          (List.exists
             (fun (part : Awskit_s3.Multipart.Part.t) ->
               part.part_number = spec.part_number)
             uploaded_parts))

  let complete_multipart conn ~bucket ~key ~upload_id ~options upload parts =
    let parts = sort_parts parts in
    match
      S3.Multipart.complete_upload conn ~bucket ~key ~upload_id parts
        ~options:options.Awskit_s3.Transfer.complete_options
    with
    | Error _ as error -> error
    | Ok complete -> Ok { Awskit_s3.Transfer.upload; parts; complete }

  let resume_multipart_upload_file conn ~bucket ~key ~upload_id ?options
      ?on_progress ~path () =
    let options =
      Option.value ~default:Awskit_s3.Transfer.default_upload_options options
    in
    let* () = Awskit_s3.Transfer.validate_upload_options options in
    let* () = Awskit_s3.Transfer.validate_upload_multipart_selection options in
    let* content_length = regular_file_length path in
    let* specs = multipart_specs ~content_length ~part_size:options.part_size in
    let* upload = Awskit_s3.Multipart.Upload.create ~bucket ~key ~upload_id in
    let* uploaded_parts =
      matching_uploaded_parts conn ~bucket ~key ~upload_id ~options specs
    in
    let initial_completed = completed_bytes specs uploaded_parts in
    let missing = remaining_specs specs uploaded_parts in
    let* uploaded_now =
      upload_missing_parts conn ~bucket ~key ~upload_id ~options ~path
        ?on_progress ~initial_completed missing
    in
    complete_multipart conn ~bucket ~key ~upload_id ~options upload
      (uploaded_parts @ uploaded_now)

  let multipart_upload_file conn ~bucket ~key ?options ?on_progress ~path () =
    let options =
      Option.value ~default:Awskit_s3.Transfer.default_upload_options options
    in
    let* () = Awskit_s3.Transfer.validate_upload_options options in
    let* () = Awskit_s3.Transfer.validate_upload_multipart_selection options in
    let* content_length = regular_file_length path in
    let* specs = multipart_specs ~content_length ~part_size:options.part_size in
    let* created =
      S3.Multipart.create_upload conn ~bucket ~key
        ~options:options.create_options ()
    in
    let upload_id = created.upload.upload_id in
    let abort_and_return error =
      ignore
        (S3.Multipart.abort_upload conn ~bucket ~key ~upload_id
           ~options:options.abort_options ());
      Error error
    in
    match
      upload_missing_parts conn ~bucket ~key ~upload_id ~options ~path
        ?on_progress ~initial_completed:0L specs
    with
    | Error error -> abort_and_return error
    | Ok parts -> (
        match
          complete_multipart conn ~bucket ~key ~upload_id ~options
            created.upload parts
        with
        | Ok _ as result -> result
        | Error error -> abort_and_return error)

  let upload_file conn ~bucket ~key ?options ?on_progress ~path () =
    let options =
      Option.value ~default:Awskit_s3.Transfer.default_upload_options options
    in
    let* () = Awskit_s3.Transfer.validate_upload_options options in
    let* content_length = regular_file_length path in
    if Int64.compare content_length options.multipart_threshold < 0 then
      let* result =
        put_file conn ~bucket ~key ~options:options.put_options ?on_progress
          ~path ()
      in
      Ok (Awskit_s3.Transfer.Put result)
    else
      let* () =
        Awskit_s3.Transfer.validate_upload_multipart_selection options
      in
      let* result =
        multipart_upload_file conn ~bucket ~key ~options ?on_progress ~path ()
      in
      Ok (Awskit_s3.Transfer.Multipart result)

  let get_file conn ~bucket ~key ?options ?on_progress ~path () =
    let consume reader =
      try
        Eio.Path.with_open_out ~create:(`Or_truncate 0o600) path (fun file ->
            let bytes = Bytes.create buffer_size in
            let rec loop transferred =
              match
                Runtime.Response_body.read reader bytes ~off:0 ~len:buffer_size
              with
              | Error _ as error -> error
              | Ok 0 -> Ok ()
              | Ok n ->
                  Eio.Flow.write file [ Cstruct.of_bytes ~len:n bytes ];
                  let transferred = Int64.add transferred (Int64.of_int n) in
                  Option.iter (fun f -> f transferred) on_progress;
                  loop transferred
            in
            loop 0L)
      with exn -> Error (body_error "write download" path exn)
    in
    match S3.Object.get conn ~bucket ~key ?options ~consume () with
    | Error _ as error -> error
    | Ok (info, ()) -> Ok info

  let temp_download_path path =
    match Eio.Path.split path with
    | Some (dir, base) ->
        Ok Eio.Path.(dir / ("." ^ base ^ ".awskit-download.tmp"))
    | None ->
        Error
          (Awskit.Error.validation ~field:"path"
             "could not derive temporary download path")

  let unlink_if_exists path = try Eio.Path.unlink path with _ -> ()

  let with_temp_download path f =
    let* temp_path = temp_download_path path in
    unlink_if_exists temp_path;
    match f temp_path with
    | Error error ->
        unlink_if_exists temp_path;
        Error error
    | Ok value -> (
        try
          Eio.Path.rename temp_path path;
          Ok value
        with exn ->
          unlink_if_exists temp_path;
          Error (body_error "rename download" path exn))

  let head_options_of_get_options (options : Awskit_s3.Get_object.options) :
      Awskit_s3.Head_object.options =
    {
      preconditions = options.preconditions;
      version_id = options.version_id;
      checksum_mode = options.checksum_mode;
      expected_bucket_owner = options.expected_bucket_owner;
    }

  let download_range_to_path conn ~bucket ~key ~options ~path ~completed
      ?on_progress spec =
    let finish =
      Int64.add spec.offset (Int64.of_int spec.length) |> Int64.pred
    in
    let range = Awskit_s3.Range.bytes_exn ~start:spec.offset ~finish in
    let get_options =
      { options.Awskit_s3.Transfer.get_options with range = Some range }
    in
    let consume reader =
      try
        Eio.Path.with_open_out ~create:`Never path (fun file ->
            ignore (Eio.File.seek file (Optint.Int63.of_int64 spec.offset) `Set);
            let bytes = Bytes.create buffer_size in
            let rec loop remaining =
              if remaining = 0 then Ok ()
              else
                let len = min buffer_size remaining in
                match Runtime.Response_body.read reader bytes ~off:0 ~len with
                | Error _ as error -> error
                | Ok 0 ->
                    Error
                      (Awskit.Error.body
                         (Fmt.str
                            "unexpected end of response while downloading \
                             range %d"
                            spec.index))
                | Ok n ->
                    Eio.Flow.write file [ Cstruct.of_bytes ~len:n bytes ];
                    completed := Int64.add !completed (Int64.of_int n);
                    Option.iter (fun f -> f !completed) on_progress;
                    loop (remaining - n)
            in
            loop spec.length)
      with exn -> Error (body_error "write download" path exn)
    in
    match S3.Object.get conn ~bucket ~key ~options:get_options ~consume () with
    | Error _ as error -> error
    | Ok (_, ()) -> Ok ()

  let ranged_download_to_path conn ~bucket ~key ~options ?on_progress ~path
      ranges =
    let completed = ref 0L in
    let download_one spec =
      download_range_to_path conn ~bucket ~key ~options ~path ~completed
        ?on_progress spec
    in
    let rec loop ranges =
      match ranges with
      | [] -> Ok ()
      | _ ->
          let batch, rest =
            split_batch ~concurrency:options.Awskit_s3.Transfer.concurrency
              ranges
          in
          let results =
            Eio.Switch.run @@ fun sw ->
            batch
            |> List.map (fun spec ->
                Eio.Fiber.fork_promise ~sw (fun () -> download_one spec))
            |> List.map Eio.Promise.await_exn
          in
          let* () = first_error_or_unit results in
          loop rest
    in
    try
      Eio.Path.with_open_out ~create:(`Or_truncate 0o600) path (fun _ -> ());
      loop ranges
    with exn -> Error (body_error "create download" path exn)

  let download_file conn ~bucket ~key ?options ?on_progress ~path () =
    let options =
      Option.value ~default:Awskit_s3.Transfer.default_download_options options
    in
    let* () = Awskit_s3.Transfer.validate_download_options options in
    let download_with_get () =
      with_temp_download path (fun temp_path ->
          let* result =
            get_file conn ~bucket ~key ~options:options.get_options ?on_progress
              ~path:temp_path ()
          in
          Ok (Awskit_s3.Transfer.Get result))
    in
    let head_options = head_options_of_get_options options.get_options in
    let* info = S3.Object.head conn ~bucket ~key ~options:head_options () in
    match info.content_length with
    | None -> download_with_get ()
    | Some content_length
      when Int64.compare content_length options.multipart_threshold < 0
           || Int64.equal content_length 0L ->
        download_with_get ()
    | Some content_length ->
        let* ranges =
          range_specs ~content_length ~part_size:options.part_size
        in
        with_temp_download path (fun temp_path ->
            let* () =
              ranged_download_to_path conn ~bucket ~key ~options ?on_progress
                ~path:temp_path ranges
            in
            Ok (Awskit_s3.Transfer.Ranged { info; parts = List.length ranges }))
end
