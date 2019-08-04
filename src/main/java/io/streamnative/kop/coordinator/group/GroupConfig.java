/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.kop.coordinator.group;

import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * Group configuration.
 */
@Data
@Accessors(fluent = true)
@Getter
public class GroupConfig {

    private final int groupMinSessionTimeoutMs;
    private final int groupMaxSessionTimeoutMs;
    private final int groupInitialRebalanceDelayMs;

}
