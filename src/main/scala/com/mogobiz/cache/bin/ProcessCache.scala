package com.mogobiz.cache.bin

import com.typesafe.scalalogging.LazyLogging

/**
 * Process Cache provides the ability to call different URLs defined in the application.conf file.
 * It currently support ESInput and HttpOutput only.
 */
object ProcessCache extends LazyLogging {

  /**
   * Run the different jobs described in the configuration file.
   *
   * @param args need the config process path defined in an application.conf { @see src/samples/conf/}
   */
  def main(args: Array[String]) {
    if (args.length < 4) {
      logger.error(s"USAGE : ProcessCache <apiPrefix> <apiStore> <frontPrefix> <frontStore> <staticUrl:StaticUrl...>")
      System.exit(-1)
    } else {
      ProcessCacheService.run(args(0), args(1), args(2), args(3), args(4).split(":").toList)
    }
  }
}
