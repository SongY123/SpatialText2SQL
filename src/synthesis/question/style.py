"""Deterministic style and spatial phrase selection for question generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from .models import QUESTION_STYLES, SQLFeatureSummary, SpatialRelationConstraint


STYLE_DESCRIPTIONS: dict[str, str] = {
    "conversational": "Use plain benchmark-like English. A light first-person request such as 'I want to know ...' is allowed when natural.",
    "formal": "Use precise professional wording such as 'What is', 'Which', 'Return', or 'Determine', while staying short.",
    "direct": "Use a straightforward benchmark-style request, usually starting with What, Which, How many, List, Find, Return, or Show.",
    "concise": "Keep the wording compact and benchmark-like. Prefer one sentence unless a short clarification is needed.",
    "polite": "Use mild courtesy such as 'Could you' or 'Please', but keep the request brief and task-focused.",
    "analytical": "Use compact investigative wording such as 'For each', 'Compare', or 'What is the average ...' without adding commentary.",
}


SPATIAL_PHRASE_VARIANTS: dict[str, list[dict[str, Any]]] = {
    "st_contains": [
        {
            "preferred_phrase": "contain",
            "alternate_phrases": ["fully contain", "enclose"],
            "semantics_note": "Preserve containment direction exactly: the left-hand geometry contains the right-hand geometry.",
            "direction_note": "Do not reverse which entity contains which.",
            "required_keywords": ["contain", "inside"],
        }
    ],
    "st_within": [
        {
            "preferred_phrase": "within",
            "alternate_phrases": ["inside", "located in"],
            "semantics_note": "Preserve containment direction exactly: the left-hand geometry is inside the right-hand geometry.",
            "direction_note": "Do not reverse the inside/outside relationship.",
            "required_keywords": ["within", "inside"],
        }
    ],
    "st_intersects": [
        {
            "preferred_phrase": "intersect",
            "alternate_phrases": ["overlap spatially", "cross"],
            "semantics_note": "Describe an intersection relationship without naming SQL functions.",
            "required_keywords": ["intersect", "overlap"],
        }
    ],
    "st_dwithin": [
        {
            "preferred_phrase": "within {threshold} units of",
            "alternate_phrases": ["no more than {threshold} units away from", "at most {threshold} units from"],
            "semantics_note": "Preserve the distance threshold exactly.",
            "required_keywords": ["within"],
        }
    ],
    "st_distance": [
        {
            "preferred_phrase": "distance between",
            "alternate_phrases": ["how far apart", "distance from"],
            "semantics_note": "Ask about the measured distance and preserve comparison or aggregation semantics around it.",
            "required_keywords": ["distance", "far"],
        }
    ],
    "st_buffer": [
        {
            "preferred_phrase": "buffer zone of {threshold} units around",
            "alternate_phrases": ["area within {threshold} units of", "{threshold}-unit buffer around"],
            "semantics_note": "Preserve the buffer radius exactly.",
            "required_keywords": ["buffer", "within"],
        }
    ],
    "st_union": [
        {
            "preferred_phrase": "combined geometry of",
            "alternate_phrases": ["merged footprint of", "union of"],
            "semantics_note": "Describe the merged spatial extent without exposing the SQL function name.",
            "required_keywords": ["combined", "merged", "union"],
        }
    ],
    "st_area": [
        {
            "preferred_phrase": "area of",
            "alternate_phrases": ["size of", "surface area of"],
            "semantics_note": "Ask for the computed area, not the raw geometry.",
            "required_keywords": ["area", "size"],
        }
    ],
    "st_length": [
        {
            "preferred_phrase": "length of",
            "alternate_phrases": ["how long", "total length of"],
            "semantics_note": "Ask for the computed length measurement.",
            "required_keywords": ["length", "long"],
        }
    ],
    "st_touches": [
        {
            "preferred_phrase": "touch",
            "alternate_phrases": ["share a boundary with", "meet at the edge of"],
            "semantics_note": "Describe boundary contact, not overlap.",
            "required_keywords": ["touch", "boundary"],
        }
    ],
}

@dataclass
class StyleSelector:
    def build_style_plan(
        self,
        *,
        features: SQLFeatureSummary,
        total_questions: int,
        rng: np.random.Generator,
        fixed_style: str = "",
        style_weights: Mapping[str, float] | None = None,
    ) -> list[str]:
        if total_questions <= 0:
            return []
        if fixed_style:
            return [fixed_style] * total_questions
        configured = {style: max(float((style_weights or {}).get(style, 1.0)), 0.0) for style in QUESTION_STYLES}
        del features
        effective = {style: configured[style] for style in QUESTION_STYLES}
        if sum(effective.values()) <= 0:
            return ["direct"] * total_questions
        if total_questions == 1:
            return [self._sample_one(effective, rng)]

        raw = np.array([effective[style] for style in QUESTION_STYLES], dtype=float)
        raw = raw / raw.sum() * float(total_questions)
        base = np.floor(raw).astype(int)
        remainder = total_questions - int(base.sum())
        if remainder > 0:
            fractional = raw - base
            jitter = rng.random(len(QUESTION_STYLES)) * 1e-6
            ranked = sorted(
                range(len(QUESTION_STYLES)),
                key=lambda idx: (-(fractional[idx] + jitter[idx]), idx),
            )
            for idx in ranked[:remainder]:
                base[idx] += 1
        plan: list[str] = []
        for idx, style in enumerate(QUESTION_STYLES):
            plan.extend([style] * int(base[idx]))
        return plan

    @staticmethod
    def _sample_one(weights: Mapping[str, float], rng: np.random.Generator) -> str:
        values = np.array([weights[style] for style in QUESTION_STYLES], dtype=float)
        values = values / values.sum()
        index = int(rng.choice(len(QUESTION_STYLES), p=values))
        return QUESTION_STYLES[index]


@dataclass
class SpatialPhraseSelector:
    def build_constraints(
        self,
        *,
        features: SQLFeatureSummary,
        rng: np.random.Generator,
    ) -> list[SpatialRelationConstraint]:
        constraints: list[SpatialRelationConstraint] = []
        thresholds = list(features.distance_thresholds)
        threshold_index = 0
        for function_name in features.postgis_functions:
            variants = SPATIAL_PHRASE_VARIANTS.get(function_name.lower())
            if not variants:
                continue
            variant = variants[int(rng.integers(0, len(variants)))]
            threshold = thresholds[threshold_index] if threshold_index < len(thresholds) else ""
            if function_name.lower() in {"st_dwithin", "st_buffer"} and threshold:
                threshold_index += 1
            preferred_phrase = str(variant["preferred_phrase"]).format(threshold=threshold)
            alternate_phrases = [
                str(item).format(threshold=threshold)
                for item in variant.get("alternate_phrases", [])
            ]
            required_keywords = [
                str(item).format(threshold=threshold)
                for item in variant.get("required_keywords", [])
            ]
            constraints.append(
                SpatialRelationConstraint(
                    function_name=function_name,
                    preferred_phrase=preferred_phrase,
                    alternate_phrases=alternate_phrases,
                    semantics_note=str(variant.get("semantics_note") or ""),
                    threshold=threshold,
                    direction_note=str(variant.get("direction_note") or ""),
                    required_keywords=required_keywords,
                )
            )
        return constraints
