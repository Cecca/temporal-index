### A Pluto.jl notebook ###
# v0.17.1

using Markdown
using InteractiveUtils

# ╔═╡ 8befb8d0-85dc-4fd3-b6ca-62dbb7792370
using Plots

# ╔═╡ 29b541f1-cb46-495e-bb20-fb7a7a8c5d83
md"## Finding the tipping point

This notebook explores when it is convenient to use the RD-index to answer a mixed workload of range duration queries, and when it is not, compared to a B-Tree, in relation to the fraction of duration-only queries (for which the BTree is vastly superior)."

# ╔═╡ 052792d0-85d8-4862-9a51-2291f58a7e84
md"These are the performance number for the BTree, taken from Table 1 of the paper"

# ╔═╡ 4973eb03-4ba8-4f9e-83a3-16e6cc97a4b1
md"And these are the numbers for the RD-Index, in queries per second."

# ╔═╡ 504cb09c-4547-43ec-ab1a-d83edd7cd532
md"And finally the performance of the interval tree"

# ╔═╡ 6505dafb-de98-4e95-a9e5-17763470980d
md"We then obtain the estimated performance for the mixed workload by doing the following:

$\frac{1}{\rho \frac{1}{qps_{do}} + (1-\rho) \frac{1}{qps_{rd}}}$

where $qps_{rd}$ is the throughput of range-duration queries and $qpq_{do}$ is the throughput of duration only queries, and $\rho$ is the fraction of duration only queries.

The idea is that we take the sum of the time required to answer either query, weighted by the frequency of that type of query, and then invert it into a throughput.
"

# ╔═╡ acab7357-d1dc-484f-a7dc-c6112c6f2c1c
md"Here follow the results for the different datasets."

# ╔═╡ 45b3ede4-77ab-11ec-0346-57a313434368
function mixed(qps_do, qps_rd, frac_do)
	t_do = 1/qps_do
	t_rd = 1/qps_rd
	t_tot = frac_do * t_do + (1-frac_do) * t_rd
	1 / t_tot
end

# ╔═╡ 35913a48-94fe-434a-88e7-d2085d0829c9
struct Performance
	d_only
	r_only
	rd
end

# ╔═╡ ad7bbc03-a4a0-4ec5-a3e6-9aba0ccdfd57
btree = Dict(
	"Webkit" => Performance(3393, 47, 758),
	"Mimic" => Performance(2_500_000, 127, 3_347),
	"Flight" => Performance(714286, 2798, 13514),
	"Random" => Performance(43103, 8, 502)
)

# ╔═╡ 63872c79-3259-483f-8763-2b0d2ac755aa
rdindex = Dict(
	"Webkit" => Performance(2544, 416, 3159),
	"Flight" => Performance(4182, 54945, 232558),
	"Mimic" => Performance(3_560, 372, 10_246),
	"Random" => Performance(842, 415, 11737)
)

# ╔═╡ 12bd32db-8fa6-4ba9-b952-9599a7c34a2c
itree = Dict(
	"Random" => Performance(12, 272, 215),
	"Flight" => Performance(683, 51813, 43860),
	"Webkit" => Performance(157, 384, 256),
	"Mimic" => Performance(76, 347, 232)
)

# ╔═╡ 2f834075-d60e-44a5-b4c0-4ac493b4f041
function mixed(perf::Performance, frac_do, frac_ro)
	frac_rd = 1 - frac_do - frac_ro
	1 / (frac_do / perf.d_only + frac_ro / perf.r_only + frac_rd / perf.rd)
end

# ╔═╡ 9ae3637f-eb4c-4a48-bd92-ffa5d087f7d0
mixed(perf::Performance, frac_do) = mixed(perf, frac_do, 0.0)

# ╔═╡ 230d8a2a-e134-41de-9ccb-92dc82239bc6
fractions = [f for f in 0:0.001:1]

# ╔═╡ c5bc773b-c3f8-4590-a791-b3ba91d9af70
function doplot(dataset)
	perf_btree = [mixed(btree[dataset], f) for f in fractions]
	perf_rdindex = [mixed(rdindex[dataset], f) for f in fractions]
	tipping_point = findall(perf_btree .> perf_rdindex)[1]
	plot(fractions, perf_btree; label = "BTree", yaxis=:log)
	plot!(fractions, perf_rdindex; label = "RDIndex")
	vline!(fractions[[tipping_point]]; linestyle = :dash, label="",
		title = "$(dataset) - Tipping point: $(fractions[tipping_point])"
	)
end

# ╔═╡ f8a3c4f5-f4d0-4835-8497-c2d188dad1dd
doplot("Random")

# ╔═╡ b13f5277-1a04-4950-b615-cabd684d737a
doplot("Flight")

# ╔═╡ 3dba6009-f170-4e8b-8bdc-675e01384b3d
doplot("Webkit")

# ╔═╡ 489d0c69-8743-4651-8fb2-eb269a927e4f
doplot("Mimic")

# ╔═╡ 6bc945c0-26ac-41c5-8f5a-1afd39b27ab6
begin
	ρ_do = 0.7
	ρ_ro = 0.25
	dataset = "Mimic"
end

# ╔═╡ b395fd02-17f7-465e-b103-c8197bb58376
mixed(rdindex[dataset], ρ_do, ρ_ro)

# ╔═╡ bcb6973d-04bb-4070-b0e1-bd50880060c3
mixed(btree[dataset], ρ_do, ρ_ro)

# ╔═╡ 61d25a92-bcbe-49c4-b692-4cfc06bfbca4
mixed(itree[dataset], ρ_do, ρ_ro)

# ╔═╡ Cell order:
# ╟─29b541f1-cb46-495e-bb20-fb7a7a8c5d83
# ╟─052792d0-85d8-4862-9a51-2291f58a7e84
# ╠═ad7bbc03-a4a0-4ec5-a3e6-9aba0ccdfd57
# ╟─4973eb03-4ba8-4f9e-83a3-16e6cc97a4b1
# ╠═63872c79-3259-483f-8763-2b0d2ac755aa
# ╟─504cb09c-4547-43ec-ab1a-d83edd7cd532
# ╠═12bd32db-8fa6-4ba9-b952-9599a7c34a2c
# ╟─6505dafb-de98-4e95-a9e5-17763470980d
# ╟─acab7357-d1dc-484f-a7dc-c6112c6f2c1c
# ╠═f8a3c4f5-f4d0-4835-8497-c2d188dad1dd
# ╠═b13f5277-1a04-4950-b615-cabd684d737a
# ╠═3dba6009-f170-4e8b-8bdc-675e01384b3d
# ╠═489d0c69-8743-4651-8fb2-eb269a927e4f
# ╠═c5bc773b-c3f8-4590-a791-b3ba91d9af70
# ╠═45b3ede4-77ab-11ec-0346-57a313434368
# ╠═2f834075-d60e-44a5-b4c0-4ac493b4f041
# ╠═9ae3637f-eb4c-4a48-bd92-ffa5d087f7d0
# ╠═35913a48-94fe-434a-88e7-d2085d0829c9
# ╠═8befb8d0-85dc-4fd3-b6ca-62dbb7792370
# ╠═230d8a2a-e134-41de-9ccb-92dc82239bc6
# ╠═6bc945c0-26ac-41c5-8f5a-1afd39b27ab6
# ╠═b395fd02-17f7-465e-b103-c8197bb58376
# ╠═bcb6973d-04bb-4070-b0e1-bd50880060c3
# ╠═61d25a92-bcbe-49c4-b692-4cfc06bfbca4
