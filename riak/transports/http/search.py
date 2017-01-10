# Copyright 2010-present Basho Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class XMLSearchResult(object):
    # Match tags that are document fields
    fieldtags = ['str', 'int', 'date']

    def __init__(self):
        # Results
        self.num_found = 0
        self.max_score = 0.0
        self.docs = []

        # Parser state
        self.currdoc = None
        self.currfield = None
        self.currvalue = None

    def start(self, tag, attrib):
        if tag == 'result':
            self.num_found = int(attrib['numFound'])
            self.max_score = float(attrib['maxScore'])
        elif tag == 'doc':
            self.currdoc = {}
        elif tag in self.fieldtags and self.currdoc is not None:
            self.currfield = attrib['name']

    def end(self, tag):
        if tag == 'doc' and self.currdoc is not None:
            self.docs.append(self.currdoc)
            self.currdoc = None
        elif tag in self.fieldtags and self.currdoc is not None:
            if tag == 'int':
                self.currvalue = int(self.currvalue)
            self.currdoc[self.currfield] = self.currvalue
            self.currfield = None
            self.currvalue = None

    def data(self, data):
        if self.currfield:
            # riak_solr_output adds NL + 6 spaces
            data = data.rstrip()
            if self.currvalue:
                self.currvalue += data
            else:
                self.currvalue = data

    def close(self):
        return {'num_found': self.num_found,
                'max_score': self.max_score,
                'docs': self.docs}
