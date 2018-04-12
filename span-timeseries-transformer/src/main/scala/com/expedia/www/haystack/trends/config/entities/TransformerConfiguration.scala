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
package com.expedia.www.haystack.trends.config.entities

import com.expedia.www.haystack.commons.entities.encodings.Encoding

/**
  * @param encoding                                config for encoding type in metric point key
  * @param enableMetricPointServiceLevelGeneration config for also generating service level trends
  */
case class TransformerConfiguration(encoding: Encoding,
                                    enableMetricPointServiceLevelGeneration: Boolean,
                                    blacklistedServices: List[String])
