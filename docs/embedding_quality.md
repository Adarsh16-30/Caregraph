# Do the embeddings carry clinical signal?

Until now the only quality number for the deployed models was their training loss
(unsupervised link prediction). That says the model learned to tell real edges from random
ones. It does not say the embeddings are useful. This page measures it, on the real
Diabetes 130 graph, with the deployed models, using a downstream task.

`ml/eval_readmission.py` is the harness. Results for the deployed models are in
`benchmarks/results/gate/prod11_readmission_eval_graphsage.json` and `..._gat.json`; results
for the richer-feature retrain described below (not deployed) are in the `..._v2.json` files
beside them.

## The test

**Task.** Predict whether an encounter is followed by a readmission within 30 days
(`readmitted == "<30"`, about 11% of encounters), using only what the graph knew at the end of
that encounter.

**Causal.** Each prediction uses the graph as of the end of the encounter's own edges. The
patient's later encounters, which are what the label is about, are not in that graph. Using
the final graph instead would leak the answer, since a readmission is another encounter node
on the same patient. The embeddings come from the deployed `model.pt`, over each
encounter's own as-of two-hop neighbourhood, built the way `resolver.rs` builds it (live
neighbours, newest first, capped).

**Split and metric.** 20,000 randomly sampled encounters (seed 0), ordered by time: the
first 70% train, the last 30% test. Logistic regression on each feature set. AUC on the test
set with a 95% bootstrap interval (200 resamples), and paired AUC differences.

**Feature sets.** *structural*: the patient's prior encounters and counts of conditions,
drugs, providers and lab results at that time. *clinical*: the encounter's own recorded fields
(stay length, number of drugs and diagnoses, admission and discharge codes). *embedding*: the
deployed embedding of the encounter and of its patient.

## Result

Test-set AUC (95% interval). A majority-class predictor scores 0.500.

| features | GraphSAGE | GAT |
|---|---|---|
| structural | 0.633 (0.610-0.655) | 0.633 (0.610-0.655) |
| clinical | 0.600 (0.580-0.623) | 0.600 (0.580-0.623) |
| embedding, encounter | 0.618 (0.597-0.640) | 0.578 (0.558-0.602) |
| embedding, patient | 0.612 (0.592-0.635) | 0.609 (0.591-0.632) |
| embedding, both | 0.618 (0.596-0.641) | 0.628 (0.609-0.649) |
| clinical + structural | 0.657 (0.632-0.678) | 0.657 (0.632-0.678) |
| clinical + embedding | 0.656 (0.632-0.678) | 0.664 (0.640-0.685) |
| clinical + structural + embedding | 0.661 (0.638-0.684) | 0.663 (0.641-0.684) |

Paired differences in AUC (95% interval):

| comparison | GraphSAGE | GAT |
|---|---|---|
| embedding vs structural | -0.015 (-0.032 to +0.006) | -0.005 (-0.019 to +0.007) |
| clinical + embedding vs clinical | +0.056 (+0.038 to +0.073) | +0.064 (+0.047 to +0.080) |
| clinical + structural + embedding vs clinical + structural | +0.004 (-0.002 to +0.011) | +0.006 (+0.002 to +0.010) |

How much of the embedding is explained by nothing but the type mix of the patient's
neighbourhood (test R squared of a least-squares fit from the six type fractions and log
degree):

| | GraphSAGE | GAT |
|---|---|---|
| patient embedding, linear fit | 0.996 | 0.989 |
| patient embedding, quadratic fit | 0.999 | 0.997 |
| encounter embedding, quadratic fit | 0.997 | 0.959 |

## What this says

1. **The embeddings carry real signal.** AUC 0.62-0.63 against 0.50, with intervals well clear
   of it. They are as predictive as the encounter's own clinical fields (0.600) and add a
   significant 0.06 on top of them.
2. **All of that signal is structural.** The five counts in the *structural* set predict as well
   (0.633), and the embeddings add nothing measurable to them for GraphSAGE (+0.004, interval
   includes zero). GAT adds a small +0.006, borderline (interval +0.002 to +0.010).
3. **The reason is visible.** 96-99.9% of the embedding's variance is explained by the type
   composition of the patient's neighbourhood. That is what the models were given: their only
   input feature is a one-hot node type (`manifest: "one-hot node type; no clinical
   attributes as features"`). The embedding is a smooth summary of how many encounters,
   conditions, drugs and lab results a patient has. It cannot encode *which* condition or drug.
   A plausible reason it scores at all is the patient's prior-encounter count, which the
   embedding recovers through the encounter fraction of the neighbourhood; prior visits
   are widely reported as strong readmission predictors, but this page did not test that
   explanation, only that the structural counts account for the signal.

So the embeddings are not wrong, and the similarity and point-in-time machinery built on them
works. What they do not yet contain is clinical content. For a clinical use that needs more
than "this patient has a lot of encounters", the models need more input than node type.

## What this does not say

* **Absolute AUCs are modest for every method here.** The loader keeps only some encounter
  fields; it drops the source's prior-visit counts (`number_inpatient` and others), which are
  the strongest known predictors. Compare the methods with each other, not with the
  literature.
* **The timeline is derived** (dates are spread across 1999-2008 by encounter order), so the
  causal cut is by order, which is real, rather than by true date.
* **One split, one dataset, one sample of 20,000.** The intervals are bootstrap intervals over
  the test set, not over different splits or samples.
* **Second-ring neighbours are sampled** (16 per node, `--ring2`) rather than the server's 512.
  A hub's neighbours are mostly patients, so the composition they contribute barely changes,
  but that was reasoned, not tested here. It matters more for GAT, whose attention is
  sensitive to which neighbours are present, so read the GAT column with that in mind.
* **It measures the models that are deployed.** It is not evidence that no embedding could
  do better.

## What would fix it, and how to know it worked

Give the models something to learn from beyond node type: the identity of conditions,
medications and lab results (their codes), and an encounter's own recorded fields as node
features.

The bar to apply: re-run this harness with `--model <new model>` and require the embedding to
beat **clinical + structural** with a paired interval that excludes zero. An embedding that
merely rebuilds degree counts has not earned its storage.

```
python ml/eval_readmission.py --trace benchmarks/traces/diabetes130_full.jsonl \
    --model diabetes130_graphsage --samples 20000
```

## Attempt 1: richer node features (done; did not clear the bar)

`ml/feature_spec.json` (built by `ml/build_feature_spec.py` from the trace's own value
vocabularies, derivation reviewable, label and encrypted fields refused by construction —
see `ml/features.py`'s module doc) replaces the one-hot node type with a 108-dimensional
vector: the type one-hot, plus, per type, the fields that were sitting unused in the trace —
an encounter's admission/discharge/source codes and its numeric fields (stay length, counts
of procedures/medications/diagnoses/lab procedures), a medication's drug identity, a
provider's specialty, a lab result's assay and result, and a condition's ICD-9 chapter (its
numbered chapter, V, E, or other — not the raw code, which has 915 distinct values on this
graph and would mostly be memorised rather than generalised from). `readmitted` and the
patient's encrypted fields (`age`, `gender`, `race`, `weight`) are on the forbidden list
`check_spec` enforces.

`train_graphsage.py --features v2` and `train_gat.py --features v2` retrain on it; both
converge (loss improves, the trainer's own gate that refuses to deploy otherwise). Results:
`ml/deployed/diabetes130_graphsage_v2/`, `ml/deployed/diabetes130_gat_v2/`,
`benchmarks/results/gate/prod11_readmission_eval_{graphsage,gat}_v2.json`.

| comparison | GraphSAGE v2 | GAT v2 |
|---|---|---|
| embedding (both) vs structural | -0.002 (-0.025 to +0.025) | +0.002 (-0.020 to +0.028) |
| clinical + structural + embedding vs clinical + structural | +0.005 (-0.003 to +0.013) | +0.005 (+0.0002 to +0.011) |

Neither clears the pre-registered bar. GraphSAGE's interval includes zero outright. GAT's
excludes it, but only just — the lower bound is +0.0002, not a result to lean on. Read both
as **no clear win**, not as "GAT passed and GraphSAGE didn't": the two point estimates
(+0.005, +0.005) are themselves indistinguishable.

**What did change:** how much of the embedding's own variance the neighbourhood's type mix
alone explains dropped sharply — GraphSAGE's patient embedding from 0.996 (linear fit) to
0.457; GAT's from 0.989 to 0.616. The embeddings are demonstrably no longer just a
reconstruction of neighbour-type counts; richer inputs changed what they represent. That
change did not show up as better 30-day-readmission prediction beyond clinical + structural.

**Reading this result.** Three explanations are consistent with it, and this page does not
distinguish between them: the signal this task needs may not be reachable from a two-hop
neighbourhood of this particular derived dataset at all (the loader drops
`number_inpatient` and the other fields known to carry most of the real predictive power —
see "What this does not say" above, which still applies); an unsupervised link-prediction
objective may not be the right pressure to surface it even when the features are present
(nothing here supervises the model to care about readmission); or a different architecture,
more depth, or more training may be needed. Plain node-level readmission prediction
(logistic regression directly on the feature vectors, no graph, no message passing) was not
tried either, so whether the message passing itself is pulling its weight is still unmeasured.

**What is and is not deployed.** `diabetes130_graphsage_v2` and `diabetes130_gat_v2` are
committed as evidence, not as a replacement: `src/config.rs`'s `DEFAULT_GRAPHSAGE_MODEL` /
`DEFAULT_GAT_MODEL` are untouched, so nothing in the running server uses them unless a
future change deliberately points at them. Given the result above, that change is not
justified yet.

The `--features v2` flag and the `ml/feature_spec.json` contract are retained either way:
they are needed for the next attempt (a supervised objective, or the missing fields added
back to the loader) to start from richer inputs rather than repeating this one.

## Where this dataset's ceiling actually is, and attempt 2: the fields the loader drops

Published results on the UCI Diabetes 130-US Hospitals dataset — the same dataset this page
uses — cluster around AUC 0.65-0.69 regardless of method: a calibrated stacking ensemble at
0.664-0.688 ([Salim & Ibrahim](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC12085305/)), random
forest at 0.684, XGBoost at 0.667, logistic regression with oversampling at 0.649 (summarised
in the [GitHub reproduction](https://github.com/chaitra-satheesh/hospital-readmission-prediction)
and a [Research Square preprint](https://www.researchsquare.com/article/rs-10560805), which
argues this ceiling is typical of structured EHR data generally, not an artifact of this
dataset's preprocessing). CareGraph's own `clinical + structural + embedding` (0.661) already
sits inside that range — the earlier "What this does not say" caveat that CareGraph's absolute
numbers should not be compared with the literature turns out, once that literature is actually
read, not to cut against CareGraph: it is close to the field's own ceiling on this data,
without the fields below.

**The published strongest predictors are not reachable from the trace, and never were.**
`data/diabetes130_loader.py`'s raw CSV has `number_inpatient`, `number_outpatient` and
`number_emergency` (counts of each visit type in the year before this encounter) as columns
16-18. `emit()`'s `encounter_props` lists nine fields to carry into the trace, and these three
are not among them — not because they are unavailable or because including them would leak
the label (they describe history strictly before this encounter, the same causal footing as
`time_in_hospital`), but because no one wired them through. This is a real, previously
undiagnosed gap behind the "What this does not say" caveat about the missing fields, not a
property of the dataset.

`ml/eval_prior_utilization.py` tests what adding them back would do, **without changing the
loader, the committed trace, or any deployed model** — a change to either of those invalidates
every gated benchmark and deployed-model manifest that depends on them, which is not a change
to make to test a hypothesis. It maps each raw CSV row to the encounter node id the existing
trace already gave it, by replaying `read_encounters`'s exact sort and `Namespace`'s exact
assignment (both already in the loader; this does not reimplement them differently), and
checks that replay against a field that *did* make it into the trace before trusting it.
Results: `benchmarks/results/gate/prod11_readmission_eval_prior_utilization_probe.json`.

**Part 1 — same methodology as everywhere else on this page** (fixed `l2=1e-3`, 95% bootstrap
interval, `diabetes130_graphsage`):

| comparison | delta AUC |
|---|---|
| clinical2 (+ prior inpatient/outpatient/emergency) vs clinical | +0.043 [+0.028, +0.057] |
| clinical2 + structural + embedding vs clinical + structural + embedding | **-0.017** [-0.027, -0.005] |

The fields are exactly as predictive alone as the literature says — clinical alone jumps from
0.600 to 0.644, a clean, well-separated gain. Added on top of everything CareGraph already
has, they make the full model measurably *worse*, not better: a significant decrease, not
noise.

**Why, and a methodology problem this surfaces.** `structural`'s own prior-encounter count and
the embedding's own neighbourhood-type-mix component already correlate strongly with
inpatient/outpatient/emergency history — a patient with many prior encounters structurally is
largely the same patient with high `number_inpatient`. A regularization sweep
(`l2` from 1e-4 to 1.0, selected by looking directly at the test set — informative about the
feature space's capacity, not trustworthy as a number to report) shows AUC moving by as much
as 0.025 for `clinical + structural + embedding` alone just from `l2`, which is comparable to
or larger than several of the differences this whole page reports as significant, including
attempt 1's. **This page's fixed, never-tuned `l2=1e-3` is a real confound, not just for this
probe.** Properly done — `l2` selected on a held-out validation slice the final number never
sees, not by looking at the test set — the result holds up and gets worse, not better:

| | selected l2 | validation AUC | **test AUC** |
|---|---|---|---|
| clinical + structural + embedding | 0.0001 | 0.639 | **0.657** |
| clinical2 + structural + embedding | 0.0001 | 0.658 | **0.638** |

Two independent procedures — the paired bootstrap at the page's standard `l2`, and this
honest validation-selected comparison — agree: adding the loader's missing fields to the full
stack does not help, and under proper model selection it clearly hurts.

**What this does and does not conclude.** The missing fields are real and the literature's
claim about them holds on their own; simply wiring them into the loader and retraining is
**not shown to be the fix** — the bottleneck is not that this signal is entirely absent, it
is that a correlated version of it already reaches the model through `structural` and the
embedding, and a third copy destabilises a logistic fit at this feature-space size more than
it helps. This does not rule out a different model class handling the redundancy better
(gradient boosting, which several of the cited published results use, is far less sensitive
to correlated inputs than L2-penalized logistic regression), nor does it rule out the fields
mattering once `build_model_input` (not just this evaluation script) actually uses them as
GNN input rather than as an evaluation-time logistic-regression feature. Neither was tried.
**Not done, deliberately:** changing `data/diabetes130_loader.py`, regenerating
`benchmarks/traces/diabetes130_full.jsonl`, or retraining/redeploying any model on it — that
is a large, hard-to-reverse step this page's evidence does not clearly justify yet, and it
would need a decision, not an assumption, before it happens.
