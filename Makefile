default: paper

.PHONY: analysis
analysis:
	R -e "drake::r_make()"

paper: analysis
	cd paper; latexmk -pdf temporal-index.tex

serve:
	cd docs; python -m SimpleHTTPServer 8888 .

tar:
	tar cvf indexing-temporal-relations.tar.gz \
		Cargo.lock \
		Cargo.toml \
		Dockerfile \
		Makefile \
		R \
		README.md \
		_drake.R \
		build.rs \
		check \
		dockerrun \
		docs \
		experiments \
		renv \
		renv.lock \
		rust-toolchain \
		src \
		temporal-index-results.sqlite



