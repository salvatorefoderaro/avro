# AVRO

Per l'esecuzione di PIT su AVRO, è stato impostato un profilo nel file *pom.xml* del sottomodulo **AVRO** con la configurazione necessaria. Il comando per l'esecuzione è dunque il seguente:

- mvn org.pitest:pitest-maven:mutationCoverage -Ppit-mutation-coverage