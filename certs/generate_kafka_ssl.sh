#!/bin/bash
set -e
set -x

# Controlla se keytool è disponibile
if ! command -v keytool &> /dev/null; then
    echo "Errore: keytool non è disponibile nel PATH."
    echo "Installa il Java Development Kit (JDK) e aggiungi il percorso della directory bin al PATH."
    exit 1
fi

# Recupera la password dalla variabile STORE_PASS (se non definita, la genero casualmente)
STORE_PASS="${STORE_PASS:-$(openssl rand -base64 16)}"
VALID_DAYS=730  # Validità in gg

echo "Uso password per keystore/truststore: "
sleep 1

echo "==== 1) Creazione della chiave privata e certificato CA self-signed ===="
openssl genrsa -out ca.key 2048
openssl req -x509 -new -key ca.key -sha256 -days $VALID_DAYS \
  -out ca.crt \
  -subj '//C=IT/ST=State/L=City/O=MyOrg/OU=Prod/CN=My-Local-CA'



echo "==== 2) Creazione del keystore (kafka.keystore.jks) ===="
keytool -genkey -noprompt \
  -alias kafka-ssl \
  -dname "CN=kafka, OU=Prod, O=MyOrg, L=City, ST=State, C=IT" \
  -keystore kafka.keystore.jks \
  -storepass "$STORE_PASS" \
  -keypass "$STORE_PASS" \
  -keyalg RSA \
  -validity $VALID_DAYS

echo "==== 3) Generazione della CSR (kafka.csr) ===="
keytool -certreq -alias kafka-ssl \
  -keystore kafka.keystore.jks \
  -storepass "$STORE_PASS" \
  -file kafka.csr

echo "==== 4) Firma della CSR con la CA per creare kafka.crt ===="
openssl x509 -req -sha256 -in kafka.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out kafka.crt -days $VALID_DAYS

echo "==== 5) Import della CA nel keystore ===="
keytool -import -noprompt \
  -alias my-ca \
  -file ca.crt \
  -keystore kafka.keystore.jks \
  -storepass "$STORE_PASS"

echo "==== 6) Import del certificato firmato (kafka.crt) nel keystore ===="
keytool -import -noprompt \
  -alias kafka-ssl \
  -file kafka.crt \
  -keystore kafka.keystore.jks \
  -storepass "$STORE_PASS"

echo "==== 7) Creazione del truststore (kafka.truststore.jks) ===="
keytool -import -noprompt \
  -alias my-ca \
  -file ca.crt \
  -keystore kafka.truststore.jks \
  -storepass "$STORE_PASS"

echo "=== Generazione completata ==="
echo "I seguenti file sono stati creati nella cartella:"
echo "  - ca.key"
echo "  - ca.crt"
echo "  - kafka.keystore.jks"
echo "  - kafka.truststore.jks"
echo "  - kafka.csr e kafka.crt"
