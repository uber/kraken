1. Create a self-signed certificate for server:
# First create private key.
$ openssl genrsa -aes256 -out server.key 4096
# Create a sign request.
$ openssl req -new -key server.key -out server.csr
```
Enter pass phrase for server_private.pem:
You are about to be asked to enter information that will be incorporated
into your certificate request.
What you are about to enter is what is called a Distinguished Name or a DN.
There are quite a few fields but you can leave some blank
For some fields there will be a default value,
If you enter '.', the field will be left blank.
-----
Country Name (2 letter code) [AU]:US
State or Province Name (full name) [Some-State]:CA
Locality Name (eg, city) []:San Francisco
Organization Name (eg, company) [Internet Widgits Pty Ltd]:Uber
Organizational Unit Name (eg, section) []:cluster-mgmt
Common Name (e.g. server FQDN or YOUR name) []:kraken
Email Address []:

Please enter the following 'extra' attributes
to be sent with your certificate request
A challenge password []:
An optional company name []:
```
# Generate cert
$ openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt

2. Create an intermediate certificate for client:
$ openssl genrsa -aes256 -out client.key 4096
$ openssl req -new -key client.key -out client.csr
```
Enter pass phrase for client.key:
You are about to be asked to enter information that will be incorporated
into your certificate request.
What you are about to enter is what is called a Distinguished Name or a DN.
There are quite a few fields but you can leave some blank
For some fields there will be a default value,
If you enter '.', the field will be left blank.
-----
Country Name (2 letter code) [AU]:US
State or Province Name (full name) [Some-State]:CA
Locality Name (eg, city) []:San Francisco
Organization Name (eg, company) [Internet Widgits Pty Ltd]:Uber
Organizational Unit Name (eg, section) []:kraken
Common Name (e.g. server FQDN or YOUR name) []:kraken
Email Address []:

Please enter the following 'extra' attributes
to be sent with your certificate request
A challenge password []:
An optional company name []:
```
Notice the difference in Organizational Unit Name. I think at least one of the names should be different from server.crt otherwise it would be treated as a self-signed key.
$ openssl x509 -req -days 365 -in client.csr -CA server.crt -CAkey server.key -CAcreateserial -out client.crt

3. Verify
$ openssl verify -verbose -CAfile server.crt client.crt

4. Decrypt client key (because curl does not support encrypted key)
$ openssl rsa -in client.key -out client_decrypted.key 

5. Both client and server should enforce verification.
- `InsecureSkipVerify` should be `false` in client and `ClientAuth` should be equal to `tls.RequireAndVerifyClientCert` in tls.Config.
- In nginx config, `ssl_verify_client` should be `on`.
