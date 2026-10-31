# Do the embeddings carry clinical signal?

Until now the only quality number for the deployed models was their training loss
(unsupervised link prediction). That says the model learned to tell real edges from random
ones. It does not say the embeddings are useful. This page measures it, on the real
Diabetes 130 graph, with the deployed models, using a downstream task.

`ml/eval_readmission.py` is the harness. Results are in
`benchmarks/results/gate/prod11_readmission_eval_graphsage.json` and
`..._gat.json`.

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
features. That is a change to `build_model_input` in `resolver.rs` (the interface contract
with the trainers' `NODE_TYPES`), to the trainers and manifests, and a retrain, and it has to
keep the incremental path exact. It has not been done.

The bar to apply when it is: re-run this harness with `--model <new model>` and require the
embedding to beat **clinical + structural** with a paired interval that excludes zero. Until
then, an embedding that merely rebuilds degree counts has not earned its storage.

```
python ml/eval_readmission.py --trace benchmarks/traces/diabetes130_full.jsonl \
    --model diabetes130_graphsage --samples 20000
```
