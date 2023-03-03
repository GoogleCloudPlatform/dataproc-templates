# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from util.notebook_functions import split_list


def test_split_list():
    for input_list, split_size, expected_outcome in [
        (
            [1, 2, 3, 4, 5, 6], 3, [[1, 2, 3], [4, 5, 6]],
        ),
        (
            ['tab1', 'tab2', 'tab3', 'tab4', 'tab5', 'tab6'], 3,
            [['tab1', 'tab2', 'tab3'], ['tab4', 'tab5', 'tab6']],
        ),
        (
            [1, 2, 3, 4, 5, 6], 2, [[1, 2], [3, 4], [5, 6]],
        ),
        (
            [1, 2, 3, 4, 5, 6], 7, [[1, 2, 3, 4, 5, 6]],
        ),
        (
            [1], 7, [[1]],
        ),
        (
            [], 7, [],
        ),
    ]:
        outcome = split_list(input_list, split_size)
        assert outcome == expected_outcome
