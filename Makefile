default: paper

.PHONY: analysis
analysis:
	R -e "drake::r_make()"

paper: analysis
	cd paper; latexmk -pdf temporal-index.tex