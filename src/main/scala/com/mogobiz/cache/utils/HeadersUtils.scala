package com.mogobiz.cache.utils

import spray.http.HttpHeaders.{RawHeader, `Content-Type`}
import spray.http._

object HeadersUtils {

  val buildHeader : PartialFunction[(String,String),HttpHeader] = {case (key, value) =>
    key.toUpperCase match {
      //        case "ACCEPT" => HttpHeaders.Accept
      //        case "ACCEPT-CHARSET" => HttpHeaders.`Accept-Charset`
      //        case "ACCEPT-ENCODING" => HttpHeaders.`Accept-Encoding`
      //        case "ACCEPT-LANGUAGE" => HttpHeaders.`Accept-Language`
      //        case "ACCESS-CONTROL-ALLOW-CREDENTIALS" => HttpHeaders.`Access-Control-Allow-Credentials`
      //        case "ACCESS-CONTROL-ALLOW-HEADERS" => HttpHeaders.`Access-Control-Allow-Headers`
      //        case "ACCESS-CONTROL-ALLOW-METHODS" => HttpHeaders.`Access-Control-Allow-Methods`
      //        case "ACCESS-CONTROL-ALLOW-ORIGIN" => HttpHeaders.`Access-Control-Allow-Origin`
      //        case "ACCESS-CONTROL-REQUEST-HEADERS" => HttpHeaders.`Access-Control-Request-Headers`
      //        case "ACCESS-CONTROL-REQUEST-METHOD" => HttpHeaders.`Access-Control-Request-Method`
      //        case "ACCESS-CONTROL-EXPOSE-HEADERS" => HttpHeaders.`Access-Control-Expose-Headers`
      //        case "ACCESS-CONTROL-MAX-AGE" => HttpHeaders.`Access-Control-Max-Age`
      //        case "ALLOW" => HttpHeaders.Allow
      //        case "ACCEPT-RANGES" => HttpHeaders.`Accept-Ranges`
      //        case "AUTHORIZATION" => HttpHeaders.Authorization
      //        case "CACHE-CONTROL" => HttpHeaders.`Cache-Control`
      //        case "CONNECTION" => HttpHeaders.Connection
      //        case "CONTENT-DISPOSITION" => HttpHeaders.`Content-Disposition`
      //        case "CONTENT-ENCODING" => HttpHeaders.`Content-Encoding`
      //        case "CONTENT-LENGTH" => HttpHeaders.`Content-Length`
      //        case "CONTENT-RANGE" => HttpHeaders.`Content-Range`
      case "CONTENT-TYPE" => buildContentType(value)
      //        case "COOKIE" => HttpHeaders.Cookie
      //        case "DATE" => HttpHeaders.Date
      //        case "ETAG" => HttpHeaders.ETag
      //        case "EXPECT" => HttpHeaders.Expect
      //        case "HOST" => HttpHeaders.Host
      //        case "IF-MATCH" => HttpHeaders.`If-Match`
      //        case "IF-MODIFIED-SINCE" => HttpHeaders.`If-Modified-Since`
      //        case "IF-NONE-MATCH" => HttpHeaders.`If-None-Match`
      //        case "IF-RANGE" => HttpHeaders.`If-Range`
      //        case "IF-UNMODIFIED-SINCE" => HttpHeaders.`If-Unmodified-Since`
      //        case "LAST-MODIFIED" => HttpHeaders.`Last-Modified`
      //        case "LOCATION" => HttpHeaders.Location
      //        case "ORIGIN" => HttpHeaders.Origin
      //        case "LINK" => HttpHeaders.Link
      //        case "RANGE" => HttpHeaders.Range
      //        case "PROXY-AUTHENTICATE" => HttpHeaders.`Proxy-Authenticate`
      //        case "PROXY-AUTHORIZATION" => HttpHeaders.`Proxy-Authorization`
      //        case "RAW-REQUEST-URI" => HttpHeaders.`Raw-Request-URI`
      //        case "REMOTE-ADDRESS" => HttpHeaders.`Remote-Address`
      //        case "SERVER" => HttpHeaders.Server(value)
      //        case "SET-COOKIE" => HttpHeaders.`Set-Cookie`
      //        case "TRANSFER-ENCODING" => HttpHeaders.`Transfer-Encoding`(value)
      case "USER-AGENT" => HttpHeaders.`User-Agent`(value)
      //        case "WWW-AUTHENTICATE" => HttpHeaders.`WWW-Authenticate`
      //        case "X-FORWARDED-FOR" => HttpHeaders.`X-Forwarded-For`
      //        case "SSL-SESSION-INFO" => HttpHeaders.`SSL-Session-Info`
      case _ => RawHeader(key, value)
    }
  }

  /**
   * @param contentTypeStatement
   * @return content-type header built from a string. Eg: "application/json; charset=UTF-8"
   */
  private def buildContentType(contentTypeStatement: String): `Content-Type` = {
    val (mediaType, charsetHeader) = contentTypeStatement.span(_ != ';')
    val (mainType, subType) = mediaType.span(_ != '/')
    val finalMediaType: MediaType = (MediaTypes.getForKey(mainType -> subType.drop(1)) match {
      case Some(m) => m
      case _ => MediaType.custom(mainType, subType.drop(1))
    })
    val charsetValue: String = charsetHeader.drop(1).split("=").drop(1).head.trim
    HttpHeaders.`Content-Type`(charsetValue.length match {
      case 0 => ContentType(finalMediaType)
      case _ => ContentType(finalMediaType, HttpCharset.custom(charsetValue))
    })
  }
}
