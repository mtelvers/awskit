open Core

module type S = sig
  module R : RUNTIME

  type connection = R.connection
  type 'a io = 'a R.t
  type upload_body = R.upload_body
  type download_reader = R.download_reader

  val bind : 'a io -> ('a -> 'b io) -> 'b io
  val return : 'a -> 'a io
  val return_ok : 'a -> ('a, Error.t) result io
  val return_error : Error.t -> ('a, Error.t) result io
  val endpoint_config : connection -> endpoint_config

  val object_request :
    connection ->
    bucket:string ->
    key:string ->
    (Endpoint_resolver.Request.t, Error.t) result

  val bucket_request :
    connection ->
    bucket:string ->
    suffix:string ->
    signing_suffix:string ->
    (Endpoint_resolver.Request.t, Error.t) result

  val root_request : connection -> (Endpoint_resolver.Request.t, Error.t) result

  val read_body :
    download_reader -> max_size:int64 -> (string, Error.t) result io

  val read_download_body :
    R.download_body -> max_size:int64 -> (string, Error.t) result io

  val discard_download_body : R.download_body -> (unit, Error.t) result io

  val with_response :
    connection ->
    method_:Awskit.Request.Method.t ->
    request:Endpoint_resolver.Request.t ->
    query:(string * string list) list ->
    headers:(string * string) list ->
    payload_hash:Awskit.Body.Payload_hash.t ->
    upload_body ->
    f:(Awskit.Response.t -> R.download_body -> ('a, Error.t) result io) ->
    ('a, Error.t) result io

  val with_empty_response :
    connection ->
    method_:Awskit.Request.Method.t ->
    request:Endpoint_resolver.Request.t ->
    query:(string * string list) list ->
    headers:(string * string) list ->
    f:(Awskit.Response.t -> R.download_body -> ('a, Error.t) result io) ->
    ('a, Error.t) result io

  val content_md5 : string -> string
end
