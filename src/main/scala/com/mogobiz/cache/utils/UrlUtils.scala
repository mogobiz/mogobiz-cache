package com.mogobiz.cache.utils

import scala.util.matching.Regex

/**
  * Created by boun on 01/07/2016.
  */
object UrlUtils {

  val extractVariablesName = "\\Q${\\E(.*?)\\Q}\\E"
  val extractVariablesNameRegex: Regex = extractVariablesName.r
  val extractVariablesRegex = "(\\Q${\\E.*?\\Q}\\E)".r

  /**
    *
    * @return StringContext of the URI.
    */
  def uriAsStringContext(uri:String): StringContext ={
    val allStaticString: List[String] = uri.split(extractVariablesName).toList
    val stringForStringContext = (extractVariablesName + "$").r.findFirstIn(uri) match {
      case Some(_) => ("" :: allStaticString.reverse).reverse
      case _ => allStaticString
    }
    new StringContext(stringForStringContext:_*)
  }

  /**
    * @return the list of all variables inside the url.
    */
  def extractUriVariablesName(uri:String): List[String] ={
    extractVariablesNameRegex.findAllMatchIn(uri).map(m => m.subgroups(0)).toList
  }

  /**
    *
    * @param url
    * @return the list of all variable name associated to its ES index and a filter as well.
    */
  def extractUriIndicesVariablesNameAndFilter(url:String): List[(String, String, String)] = {
    extractUriVariablesName(url)
      .map(subGroupMatching => {
        val (indice, fieldAndFilter) = subGroupMatching.span(_ != '.')
        val (field, filter) = if (fieldAndFilter.isEmpty) ("", "") else fieldAndFilter.tail.span(_ != '|')
        val (indiceTrimmed, fieldTrimmed, filterTrimmed) = (indice.trim, field.trim, if (filter.isEmpty) "" else filter.tail.trim)
        if (fieldTrimmed.isEmpty) {
          throw new IllegalArgumentException("The variable doesn't have any field " + subGroupMatching)
        } else {
          filterTrimmed match {
            case filter if filter.isEmpty => (indiceTrimmed, fieldTrimmed, filterTrimmed)
            case "encode" => (indiceTrimmed, fieldTrimmed, filterTrimmed)
            case f => throw new IllegalArgumentException(s"The filter ${f} doesn't exist")
          }
        }
      })
  }

  /**
    * @return the list of all variables inside the url.
    */
  def extractUriVariables(uri:String): List[String] ={
    extractVariablesRegex.findAllMatchIn(uri).map(m => m.subgroups(0)).toList
  }

  /**
    *
    * @param url
    * @return uri without space in all variables inside of it
    */
  def stripSpaceAndFiltersInVariable(url:String): String = {
    val variablesWithoutSpaceAndFilters: List[String] = extractUriVariables(url).map(_.replaceAllLiterally(" ", "").replaceAll("\\|[^}]+", ""))
    uriAsStringContext(url).s(variablesWithoutSpaceAndFilters:_*)
  }
}
