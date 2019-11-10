package com.databricks.apps.twitterClassifier

import java.util.Properties

import com.github.acrisci.commander.Program

import scala.io.Source

case class TwitterOptions(
                           consumerKey: String,
                           consumerSecret: String,
                           accessToken: String,
                           accessTokenSecret: String
                         ) extends Serializable

trait TwitterOptionParser {
  def _program = new Program()
    .version("2.0.0")


  def apply(args: Array[String]): TwitterOptions = {
    val url = getClass.getResource("build.properties")
    val properties: Properties = new Properties()
    val source = Source.fromURL(url)
    properties.load(source.bufferedReader())
    val twitterOptions = TwitterOptions(
      consumerKey = properties.getProperty("consumerKey"),
      consumerSecret = properties.getProperty("consumerSecret"),
      accessToken = properties.getProperty("accessToken"),
      accessTokenSecret = properties.getProperty("accesTokenSecret")
    )
    System.setProperty("twitter4j.oauth.consumerKey",       twitterOptions.consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret",    twitterOptions.consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken",       twitterOptions.accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", twitterOptions.accessTokenSecret)

    twitterOptions
  }
}