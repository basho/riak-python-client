#
# Copyright 2014 Basho Technologies, Inc.
#
# This file is provided to you under the Apache License,
# Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain
# a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
#under the License.

CNTRY=US
STATE=Washington
CITY=Seattle
ORG=Basho
UNIT=Riak Python Client
EMAIL=clients@basho.com
PASSWD=#testpasswd
COMPANY=Basho Technologies
DAYS=3650

SSL=openssl
SSL_CONF=openssl.cnf

CA_DIR=ca
CA_KEY=${CA_DIR}/ca.key
# Certification Signing Request
CA_CSR=${CA_DIR}/ca.csr
CA_CERT=${CA_DIR}/ca.crt
# Common Name
CA_CN=CA
CA_STRING="${CNTRY}\n${STATE}\n${CITY}\n${ORG}\n${UNIT}\n${CA_CN}\n${EMAIL}\n${PASSWD}\n${COMPANY}\n"

SERVER_DIR=server
SERVER_KEY=${SERVER_DIR}/server.key
# Certification Signing Request
SERVER_CSR=${SERVER_DIR}/server.csr
SERVER_CERT=${SERVER_DIR}/server.crt
# Certificate Revocation List
SERVER_CRL=${SERVER_DIR}/server.crl
# Common Name (must match nodename)
SERVER_CN=riak@127.0.0.1
SERVER_STRING="${CNTRY}\n${STATE}\n${CITY}\n${ORG}\n${UNIT}\n${SERVER_CN}\n${EMAIL}\n${PASSWD}\n${COMPANY}\n"

CLIENT_DIR=client
CLIENT_KEY=${CLIENT_DIR}/client.key
CLIENT_CSR=${CLIENT_DIR}/client.csr
CLIENT_CERT=${CLIENT_DIR}/client.crt
# Common Name (must name Riak username)
CLIENT_CN=certuser
CLIENT_STRING="${CNTRY}\n${STATE}\n${CITY}\n${ORG}\n${UNIT}\n${CLIENT_CN}\n${EMAIL}\n${PASSWD}\n${COMPANY}\n"

RANDOM=od -vAn -N3 -tu4 < /dev/urandom| awk '{print $1;}'
SERIAL=serial
CRL=crlnumber
NEWCERT_DIR=newcerts
INDEX=index.txt
INDEX_ATTR=index.txt.attr

all: ${CA_CERT} ${SERVER_CERT} ${CLIENT_CERT} ${SERVER_CRL}

install:
	cp ${CA_CERT} .
	cp ${SERVER_CERT} .
	cp ${SERVER_KEY} .
	cp ${SERVER_CRL} .
	cp ${CLIENT_KEY} .
	cp ${CLIENT_CERT} .

# Certificate Serial Number
${SERIAL}:
	printf "%06x" `${RANDOM}` > $@

# Certificate Revocation List Number
${CRL}:
	printf "%06x" `${RANDOM}` > $@
	
${CA_KEY}: ${CA_DIR} ${SERIAL} ${CRL} ${INDEX} ${INDEX_ATTR} ${NEWCERT_DIR}
	${SSL} genrsa -out $@ 2048

${CA_CSR}: ${CA_KEY}
	printf ${CA_STRING} | ${SSL} req -config ${SSL_CONF} -new -key $< -out $@

${CA_CERT}: ${CA_CSR}
	${SSL} x509 -req -days ${DAYS} -in $< -out $@ -signkey ${CA_KEY}

${SERVER_KEY}: ${SERVER_DIR} ${SERIAL} ${CRL}
	${SSL} genrsa -out $@ 2048

${SERVER_CSR}: ${SERVER_KEY}
	printf ${SERVER_STRING} | ${SSL} req -config ${SSL_CONF} -new -key $< -out $@

${SERVER_CERT}: ${SERVER_CSR}
	yes | OPENSSL_CONF=${SSL_CONF} ${SSL} ca -days ${DAYS} -in $< -cert ${CA_CERT} -out $@ 

${SERVER_CRL}: ${CRL} ${SERVER_CERT} ${CA_KEY} ${CA_CERT}
	rm -f ${INDEX}
	touch ${INDEX}
	OPENSSL_CONF=${SSL_CONF} ${SSL} ca -gencrl -keyfile ${CA_KEY} -cert ${CA_CERT} -out $@
	OPENSSL_CONF=${SSL_CONF} ${SSL} ca -revoke ${SERVER_CERT} -keyfile ${CA_KEY} -cert ${CA_CERT}
	OPENSSL_CONF=${SSL_CONF} ${SSL} ca -gencrl -keyfile ${CA_KEY} -cert ${CA_CERT} -out $@

${CLIENT_KEY}: ${CLIENT_DIR} ${SERIAL} ${CRL}
	${SSL} genrsa -out $@ 2048

${CLIENT_CSR}: ${CLIENT_KEY}
	printf ${CLIENT_STRING} | ${SSL} req -config ${SSL_CONF} -new -key $< -out $@

${CLIENT_CERT}: ${CLIENT_CSR}
	yes | OPENSSL_CONF=${SSL_CONF} ${SSL} ca -days ${DAYS} -in $< -cert ${CA_CERT} -key ${CA_KEY} -out $@ 

clean:
	rm -rf ${CA_DIR} ${SERVER_DIR} ${CLIENT_DIR} ${NEWCERT_DIR} ${SERIAL}* ${CRL}* ${INDEX}*

${CA_DIR}:
	mkdir -p $@	

${SERVER_DIR}:
	mkdir -p $@	

${CLIENT_DIR}:
	mkdir -p $@
	
${NEWCERT_DIR}:
	mkdir -p $@

${INDEX}:
	touch ${INDEX}

${INDEX_ATTR}:
	touch ${INDEX_ATTR}

