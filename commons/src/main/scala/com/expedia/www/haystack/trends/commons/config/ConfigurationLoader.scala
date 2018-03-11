/*
 *  Copyright 2017 Expedia, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.expedia.www.haystack.trends.commons.config

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object ConfigurationLoader {

  private val ENV_NAME_PREFIX = "HAYSTACK_PROP_"
  private val LOGGER = LoggerFactory.getLogger(ConfigurationLoader.getClass)

  /**
    * Load and return the configuration
    * if overrides_config_path env variable exists, then we load that config file and use base.conf as fallback,
    * else we load the config from env variables(prefixed with haystack) and use base.conf as fallback
    */
  lazy val loadAppConfig: Config = {

    val baseConfig = ConfigFactory.load("config/base.conf")

    val config = sys.env.get("HAYSTACK_OVERRIDES_CONFIG_PATH") match {
      case Some(path) => ConfigFactory.parseFile(new File(path)).withFallback(baseConfig)
      case _ => loadFromEnvVars().withFallback(baseConfig)
    }

    LOGGER.info(config.root().render(ConfigRenderOptions.defaults().setOriginComments(false)))
    config
  }

  /**
    * @return new config object with haystack specific environment variables
    */
  private def loadFromEnvVars(): Config = {
    val envMap = sys.env.filter {
      case (envName, _) => isHaystackEnvVar(envName)
    } map {
      case (envName, envValue) => (transformEnvVarName(envName), envValue)
    }

    ConfigFactory.parseMap(envMap.asJava)
  }

  private def isHaystackEnvVar(env: String): Boolean = env.startsWith(ENV_NAME_PREFIX)

  // converts the env variable to HOCON format
  // for e.g. env HAYSTACK_KAFKA_STREAMS_NUM_STREAM_THREADS to kafka.streams.num.stream.threads
  private def transformEnvVarName(env: String): String = {
    env.replaceFirst(ENV_NAME_PREFIX, "").toLowerCase.replace("_", ".")
  }
}
