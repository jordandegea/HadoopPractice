OUT_NAME=HADOOP_DANTIGNY_DEGEA

all:
	@echo "hum, nope"

doc: 
	cd rapport && pdflatex rapport.tex > /dev/null

package: doc
	mkdir ${OUT_NAME}
	cp README.md ${OUT_NAME}/
	cp rapport/rapport.pdf ${OUT_NAME}/
	cp -r TPIntroHadoop ${OUT_NAME}/sources
	tar -cvf ${OUT_NAME}.tar.gz ${OUT_NAME}
	rm -Rf ${OUT_NAME}

clean:
	rm -Rf ${OUT_NAME}
	rm -Rf ${OUT_NAME}.tar.gz