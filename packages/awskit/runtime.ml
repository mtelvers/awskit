module type S = sig
  type +'a t

  val return : 'a -> 'a t
  val bind : 'a t -> ('a -> 'b t) -> 'b t

  type connection
  type upload_body
  type download_body
  type upload_writer
  type download_reader

  val now : connection -> Ptime.t
  val region : connection -> Region.t
  val credentials : connection -> (Credentials.t, Error.t) result t
  val endpoint : connection -> Endpoint.t option
  val retry_policy : connection -> Retry.t
  val sleep : connection -> Ptime.Span.t -> unit t
  val empty_body : upload_body
  val string_body : string -> upload_body
  val bytes_body : bytes -> upload_body

  val stream_body :
    Body.Upload.descriptor ->
    write:(upload_writer -> (unit, Error.t) result t) ->
    upload_body

  val upload_descriptor : upload_body -> Body.Upload.descriptor
  val write_string : upload_writer -> string -> (unit, Error.t) result t

  val read :
    download_reader -> bytes -> off:int -> len:int -> (int, Error.t) result t

  val with_download_body :
    download_body ->
    consume:(download_reader -> ('a, Error.t) result t) ->
    ('a, Error.t) result t

  val discard_download_body : download_body -> (unit, Error.t) result t

  val with_response :
    connection ->
    Request.t ->
    upload_body ->
    f:(Response.t -> download_body -> ('a, Error.t) result t) ->
    ('a, Error.t) result t
end
