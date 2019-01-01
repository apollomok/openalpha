release:
	mkdir -p build/release; cd build/release; cmake ../../src -DCMAKE_BUILD_TYPE=Release; make ${args}; cd -;

debug:
	mkdir -p build/debug; cd build/debug; cmake ../../src -DCMAKE_BUILD_TYPE=Debug; make ${args}; cd -;

lint:
	./scripts/cpplint.py src/*/*h src/*/*cc

fmt:
	clang-format -style=Google -i src/*/*h src/*/*cc

clean:
	rm -rf build;
