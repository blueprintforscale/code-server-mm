"""
HCP record classifier — determines whether an estimate/job/invoice is
'treatment', 'inspection', 'canceled', or 'unknown'.

Phase 1 (this file): classify_job() only.
Later phases will add classify_estimate() and classify_invoice().

Signal priority (highest → lowest):
  1. Status (canceled → canceled)
  2. Parent job grouping (segments inherit from parent)
  3. HCP custom field `job_type` (client's own pick)
  4. Tags (client's own categorization)
  5. Line items (ALL of them, not just first)  — requires line item pull
  6. Description (filtered for HCP boilerplate)
  7. Linked estimate classification (via original_estimate_id)
  8. Amount floor

Unknown outputs route to VA review queue via review_needed flag.
"""

import re


# ─────────────────────────────────────────────────────────────────────
# Keyword sets — shared with estimates/invoices in later phases
# ─────────────────────────────────────────────────────────────────────

TREATMENT_KEYWORDS = re.compile(
    r'remediation|dry\s*fog|treatment|removal|abatement|encapsulation'
    r'|instapure|everpure|containment|demolition|demo\b|retreatment'
    r'|water\s*mitigation|mold\s*remediation|mold\s*treatment'
    r'|crawl.?space.?encapsul|crawl.?space.?door'
    r'|vapor\s*barrier|dehumidifier\s*install',
    re.IGNORECASE
)

INSPECTION_KEYWORDS = re.compile(
    r'assessment|inspection|\btest|evaluat|consult|survey|sample|sampling'
    r'|walk.?through|instascope|\bscan\b|moisture.?check|mold.?report'
    r'|clearance|ermi|visual\s+assess|air\s+quality|air\s+test'
    r'|forensic|analysis|load\s*calc|capacity\s*calc',
    re.IGNORECASE
)

# Inspection phrases that should override treatment keywords for *small* jobs
INSPECTION_PRIORITY_PHRASES = re.compile(
    r'pre.?treatment|air\s*quality\s*test|air\s*test|mold\s*test'
    r'|testing\s+and\s+estimate|visual\s+assessment|complimentary\s+estimate'
    r'|before\s*&?\s*after\s+mold\s+test',
    re.IGNORECASE
)

# HCP boilerplate descriptions that carry zero classification signal
BOILERPLATE_DESCRIPTIONS = {
    'work authorization - anticipated scope and terms & conditions',
    'work authorization',
    'terms and conditions',
    'terms & conditions',
    '',
}

# Tag-based strong signals
TREATMENT_TAGS = {
    'mold treatment', 'water mitigation', 'crawl space', 'crawl space service',
    'encapsulation', 'remediation', 'retreatment', 'warranty mold treatment',
    'containment', 'demolition', 'crawl space encapsulation',
}

# Pattern-matched treatment tags (plumber-referral workflows)
# BDR = Business Development Rep, PBR = Plumber Business Referral,
# "Plumber-*" / "Plumber Warranty" = plumber referral for water mitigation
TREATMENT_TAG_PATTERNS = [
    re.compile(r'^bdr[\s\-]', re.IGNORECASE),
    re.compile(r'^plumber[\s\-]', re.IGNORECASE),
    re.compile(r'^pbr([\s\-]|$)', re.IGNORECASE),
    re.compile(r'plumber\s*warranty', re.IGNORECASE),
]
INSPECTION_TAGS = {
    'inspection', 'assessments & testing', 'assessment', 'testing',
    'mold test', 'air quality test', 'assessment & testing',
}

# Cancelled statuses — skip from funnel entirely
CANCELED_STATUSES = {'user canceled', 'pro canceled', 'canceled'}

# Status thresholds
SMALL_JOB_CENTS = 100_000   # $1,000
LARGE_JOB_CENTS = 1_000_000 # $10,000


# ─────────────────────────────────────────────────────────────────────
# Custom-field mapping
# ─────────────────────────────────────────────────────────────────────

def _classify_linked_option(linked_option):
    """
    Classify a job based on its linked estimate option (when job was
    created from an HCP estimate and HCP set original_estimate_id).

    linked_option: {name, total_cents, tags, message_from_pro, status}
    """
    if not linked_option:
        return None

    # Tags on the option (same TREATMENT_TAGS / INSPECTION_TAGS as job-level)
    tags = {(t or '').lower().strip() for t in (linked_option.get('tags') or [])}
    if tags & TREATMENT_TAGS:
        return 'treatment'
    if tags & INSPECTION_TAGS:
        return 'inspection'
    for tag in tags:
        if any(p.search(tag) for p in TREATMENT_TAG_PATTERNS):
            return 'treatment'

    # Option name (skip "Option #1" / "Option #2" / "Option #3" boilerplate)
    name = (linked_option.get('name') or '').lower().strip()
    if name and not re.match(r'^option\s*#?\d+$', name) and name != 'copy of option #1':
        if TREATMENT_KEYWORDS.search(name):
            return 'treatment'
        if INSPECTION_PRIORITY_PHRASES.search(name):
            return 'inspection'
        if INSPECTION_KEYWORDS.search(name):
            return 'inspection'

    # message_from_pro: free-text field — only look for strong treatment phrases
    msg = (linked_option.get('message_from_pro') or '').lower()
    if msg and TREATMENT_KEYWORDS.search(msg):
        return 'treatment'

    # Amount-based fallback on the option itself
    amt = linked_option.get('total_cents') or 0
    if amt >= SMALL_JOB_CENTS:
        return 'treatment'
    if 0 < amt < SMALL_JOB_CENTS:
        return 'inspection'

    return None


def _classify_custom_field(hcp_job_type):
    """HCP `job_fields.job_type.name` → category or None if ambiguous."""
    if not hcp_job_type:
        return None
    jt = hcp_job_type.lower().strip()
    if any(k in jt for k in [
        'treatment', 'mitigation', 'encapsulation', 'remediation',
        'retreatment', 'warranty mold', 'containment', 'demolition',
    ]):
        return 'treatment'
    if any(k in jt for k in [
        'inspection', 'assessment', 'testing', 'evaluation',
    ]):
        return 'inspection'
    # 'Service', 'Plumber - Containment', or unmapped values fall through
    if 'plumber' in jt and 'containment' in jt:
        return 'treatment'
    return None


# ─────────────────────────────────────────────────────────────────────
# Main classifier
# ─────────────────────────────────────────────────────────────────────

def classify_job(
    description=None,
    tags=None,
    hcp_job_type=None,
    line_items=None,
    total_cents=0,
    status=None,
    parent_job_classification=None,
    linked_estimate=None,
    linked_option=None,
):
    """
    Returns a dict:
      {
        'category': 'treatment' | 'inspection' | 'canceled' | 'unknown',
        'review_needed': bool,
        'review_reason': str | None,
        'signal': str,   # which rule fired
      }
    """
    total_cents = total_cents or 0

    # ── 1. Status: canceled jobs don't count ────────────────────
    if status and status.lower() in CANCELED_STATUSES:
        return _result('canceled', signal='status_canceled')

    # ── 2. Segment of a parent job → inherit ────────────────────
    if parent_job_classification:
        return _result(parent_job_classification, signal='parent_segment')

    # ── 3. HCP custom field (client's own pick) ─────────────────
    cf_category = _classify_custom_field(hcp_job_type)
    if cf_category:
        return _result(cf_category, signal='custom_field')

    # ── 3b. Linked estimate option — STRONG signal when present ──
    # Jobs created from an HCP estimate reference the option's est_xxx ID.
    # The option has its own tags/name/message carrying real classification info.
    lo_category = _classify_linked_option(linked_option)
    if lo_category:
        return _result(lo_category, signal='linked_option')

    # ── 4. Tags ─────────────────────────────────────────────────
    tag_set = {t.lower().strip() for t in (tags or [])}
    has_treatment_tag = bool(tag_set & TREATMENT_TAGS)
    has_inspection_tag = bool(tag_set & INSPECTION_TAGS)

    if has_treatment_tag and not has_inspection_tag:
        return _result('treatment', signal='tags')
    if has_inspection_tag and not has_treatment_tag:
        return _result('inspection', signal='tags')

    # Pattern-matched treatment tags (Plumber-/BDR-/PBR/Plumber Warranty)
    # These are the water mitigation workflow tags — always treatment
    for tag in tag_set:
        if any(p.search(tag) for p in TREATMENT_TAG_PATTERNS):
            return _result('treatment', signal='plumber_referral_tag')
    # Conflicting tags → fall through to deeper signals

    # Dehumidifier-alone is genuinely ambiguous — flag for VA
    if tag_set == {'dehumidifier'} or (len(tag_set) == 1 and 'dehumidifier' in tag_set):
        desc = (description or '').lower()
        if 'install' in desc:
            return _result('treatment', signal='dehumidifier_install')
        if 'assess' in desc:
            return _result('inspection', signal='dehumidifier_assessment')
        return _result(
            'unknown',
            review_needed=True,
            review_reason='ambiguous_dehumidifier',
            signal='dehumidifier_ambiguous',
        )

    # ── 5. Line items (if we have them) ─────────────────────────
    # Use per-item classification + $ majority (same approach as invoices).
    # _classify_line_item() handles priority inspection phrases correctly.
    if line_items:
        treatment_cents = 0
        inspection_cents = 0
        for li in line_items:
            name = (li.get('name') or '') + ' ' + (li.get('description') or '')
            amt = li.get('amount_cents') or 0
            cat = _classify_line_item(name)
            if cat == 'treatment':
                treatment_cents += amt
            elif cat == 'inspection':
                inspection_cents += amt
        if treatment_cents > inspection_cents and treatment_cents > 0:
            return _result('treatment', signal='line_items_treatment_majority')
        if inspection_cents > treatment_cents and inspection_cents > 0:
            return _result('inspection', signal='line_items_inspection_majority')
        # All zero / tied — fall through to description and amount floor

    # ── 6. Description (filter boilerplate) ─────────────────────
    desc_raw = (description or '').strip()
    desc = desc_raw.lower()
    if desc in BOILERPLATE_DESCRIPTIONS:
        # Boilerplate + no tags + no custom field + no line items.
        # Mid-$ rescue: $1k+ jobs with no negative signals are usually real
        # treatment work (the client just used the terms doc as description).
        # BUT: if we already checked line items and they had no keyword
        # matches, don't rescue — admit uncertainty (fall to unknown).
        if total_cents >= SMALL_JOB_CENTS and not line_items:
            return _result('treatment', signal='boilerplate_mid_amount_rescue')
        # Fall through to linked estimate + amount floor
    else:
        # Priority phrases on small jobs → inspection
        if INSPECTION_PRIORITY_PHRASES.search(desc) and total_cents < SMALL_JOB_CENTS:
            return _result('inspection', signal='priority_phrase_small')

        # Treatment keywords in description
        if TREATMENT_KEYWORDS.search(desc):
            return _result('treatment', signal='description_treatment')

        # Inspection keywords without treatment
        if INSPECTION_KEYWORDS.search(desc) and not TREATMENT_KEYWORDS.search(desc):
            return _result('inspection', signal='description_inspection')

    # ── 7. Linked estimate ──────────────────────────────────────
    if linked_estimate:
        est_type = linked_estimate.get('estimate_type')
        est_approved = linked_estimate.get('approved_total_cents') or 0
        if est_type == 'treatment' and est_approved >= SMALL_JOB_CENTS:
            return _result('treatment', signal='linked_estimate_treatment')
        if est_type == 'inspection':
            return _result('inspection', signal='linked_estimate_inspection')

    # ── 8. Amount floor ─────────────────────────────────────────
    if total_cents >= LARGE_JOB_CENTS:
        return _result('treatment', signal='amount_large')
    if total_cents == 0:
        return _result(
            'unknown',
            review_needed=True,
            review_reason='zero_amount_no_signals',
            signal='zero_amount',
        )
    if total_cents < SMALL_JOB_CENTS:
        return _result('inspection', signal='amount_small')

    # ── 9. Mid-range ($1k–$10k) with no other negative signals ──
    # By now we've excluded canceled, custom-field, tags, line items,
    # description inspection keywords, and linked estimates. A mid-$
    # job with no inspection signal is almost always real treatment work
    # (especially for clients who use boilerplate descriptions).
    return _result('treatment', signal='amount_mid_default')


def _result(category, review_needed=False, review_reason=None, signal=''):
    return {
        'category': category,
        'review_needed': review_needed,
        'review_reason': review_reason,
        'signal': signal,
    }


# ─────────────────────────────────────────────────────────────────────
# Line item keyword rules (shared by invoice + future estimate classifiers)
# ─────────────────────────────────────────────────────────────────────

# Priority inspection phrases — override treatment branding.
# E.g., "Mold Remediation - Air Quality Test" has both "remediation" and
# "air quality test", but it's really a test. These phrases win.
LINE_ITEM_PRIORITY_INSPECTION = re.compile(
    r'air\s*quality\s*test|mold\s*test|tape\s*test|tape\s*sample'
    r'|petri|swab|before\s*&?\s*after\s+mold\s+test',
    re.IGNORECASE
)

LINE_ITEM_TREATMENT = re.compile(
    r'remediation|treatment|removal|abatement|encapsulation|demolition'
    r'|retreatment|dehumidifier|vapor\s*barrier|insulation|dry\s*fog|fog'
    r'|instapure|everpure|\binstall|containment|vaporshield|pure\s*install'
    r'|crawl\s*space\s*debris|janitorial|viper',
    re.IGNORECASE
)

LINE_ITEM_INSPECTION = re.compile(
    r'inspection|assessment|evaluat|consultation|\bsurvey|\bsample\b'
    r'|\btest\b|moisture\s*check|mold\s*report|clearance|ermi|visual'
    r'|walk.?through|instascope'
    r'|forensic|\banalysis\b|load\s*calc|capacity\s*calc',
    re.IGNORECASE
)


def _classify_line_item(name):
    """Return 'treatment', 'inspection', or None (unknown) for one line item."""
    if not name:
        return None
    n = name.lower()
    # Priority inspection phrases win over treatment branding
    if LINE_ITEM_PRIORITY_INSPECTION.search(n):
        return 'inspection'
    # Strong treatment keywords
    if LINE_ITEM_TREATMENT.search(n):
        return 'treatment'
    # Inspection keywords
    if LINE_ITEM_INSPECTION.search(n):
        return 'inspection'
    return None


# ─────────────────────────────────────────────────────────────────────
# Estimate classifier
# ─────────────────────────────────────────────────────────────────────

_OPTION_BOILERPLATE_NAME_RE = re.compile(
    r'^(option\s*#?\d+|copy\s+of\s+option\s*#?\d+|worksheet|standard)$',
    re.IGNORECASE,
)


def classify_estimate(
    options=None,
    status=None,
    linked_job_category=None,
    fallback_highest_option_cents=0,
):
    """
    Classify an estimate as treatment / inspection / canceled / unknown.

    An estimate has one or more options (pricing tiers). We look across ALL
    options: if ANY has a treatment signal, the estimate is 'treatment'.
    This matches the intent — "was a treatment option offered to the client?"

    Args:
      options: list of dicts with keys: name, tags, total_cents,
               message_from_pro, status, approval_status
      status: overall estimate status (canceled, sent, approved, declined)
      linked_job_category: work_category of any job created from this estimate
      fallback_highest_option_cents: top-level highest_option_cents from the
          estimate row itself, used when no option rows exist in our DB
          (data gap edge case).

    Returns: result dict (same shape as classify_job/classify_invoice)
    """
    # Skip canceled at the estimate level
    if status and status.lower() == 'canceled':
        return _result('canceled', signal='status_canceled')

    opts = list(options or [])
    # Drop canceled/declined options from consideration
    active_opts = [o for o in opts if (o.get('status') or '').lower() != 'canceled']

    if not active_opts:
        # Data-gap edge case: estimate row has highest_option_cents but no
        # rows in hcp_estimate_options (options deleted or not synced).
        # Fall back to linked job category, then the estimate's own amount.
        if linked_job_category in ('treatment', 'inspection'):
            return _result(linked_job_category, signal='no_options_linked_job')
        amt = fallback_highest_option_cents or 0
        if amt >= LARGE_JOB_CENTS:
            return _result('treatment', signal='no_options_amount_large')
        if amt >= SMALL_JOB_CENTS:
            return _result('treatment', signal='no_options_amount_mid')
        if amt > 0:
            return _result('inspection', signal='no_options_amount_small')
        return _result(
            'unknown',
            review_needed=True,
            review_reason='no_options_zero_amount',
            signal='no_options',
        )

    # ── Per-option classification ───────────────────────────────
    # For each option, decide if IT alone signals treatment. An option with
    # an inspection tag/phrase is "dirty" and we skip it, but we still look
    # at OTHER options — because clients often have multiple options per
    # estimate (one inspection + one treatment). Treatment wins over
    # inspection if ANY clean option signals treatment.
    def _option_signals_treatment(opt):
        tag_set = {(t or '').lower().strip() for t in (opt.get('tags') or [])}
        name = (opt.get('name') or '').lower().strip()
        amt = opt.get('total_cents', 0) or 0
        is_boilerplate = not name or bool(_OPTION_BOILERPLATE_NAME_RE.match(name))

        # Dirty option: has explicit inspection tag or priority phrase in name
        if tag_set & INSPECTION_TAGS:
            return None  # cannot contribute a treatment signal
        if not is_boilerplate and INSPECTION_PRIORITY_PHRASES.search(name):
            return None
        if not is_boilerplate and INSPECTION_KEYWORDS.search(name) \
                and not TREATMENT_KEYWORDS.search(name):
            return None

        # Clean option — check treatment signals
        if tag_set & TREATMENT_TAGS:
            return 'option_tags_treatment'
        for tag in tag_set:
            if any(p.search(tag) for p in TREATMENT_TAG_PATTERNS):
                return 'option_tags_plumber_referral'
        if not is_boilerplate and TREATMENT_KEYWORDS.search(name):
            return 'option_name_treatment'
        msg = (opt.get('message_from_pro') or '').lower()
        if msg and TREATMENT_KEYWORDS.search(msg):
            return 'option_message_treatment'
        # Amount alone: ≥ $1k on a clean option = treatment
        if amt >= SMALL_JOB_CENTS:
            return 'option_amount_treatment'
        return None

    # First pass: any clean option signals treatment?
    for opt in active_opts:
        sig = _option_signals_treatment(opt)
        if sig:
            return _result('treatment', signal=sig)

    # ── Fallback: inspection signals across options ─────────────
    for opt in active_opts:
        tag_set = {(t or '').lower().strip() for t in (opt.get('tags') or [])}
        if tag_set & INSPECTION_TAGS:
            return _result('inspection', signal='option_tags_inspection')

    for opt in active_opts:
        name = (opt.get('name') or '').lower().strip()
        if not name or _OPTION_BOILERPLATE_NAME_RE.match(name):
            continue
        if INSPECTION_PRIORITY_PHRASES.search(name):
            return _result('inspection', signal='option_name_priority_inspection')
        if INSPECTION_KEYWORDS.search(name):
            return _result('inspection', signal='option_name_inspection')

    # ── 6. Linked job category (if a job was created from this estimate) ──
    if linked_job_category in ('treatment', 'inspection'):
        return _result(linked_job_category, signal='linked_job')

    # ── 7. Amount floor (preserves existing amount-based behavior) ──
    highest = max(
        (o.get('total_cents', 0) or 0) for o in active_opts
    )
    if highest >= LARGE_JOB_CENTS:
        return _result('treatment', signal='amount_large')
    if highest == 0:
        return _result(
            'unknown',
            review_needed=True,
            review_reason='zero_amount_no_signals',
            signal='zero_amount',
        )
    if highest < SMALL_JOB_CENTS:
        return _result('inspection', signal='amount_small')
    # Mid-$ default → treatment (matches job classifier behavior)
    return _result('treatment', signal='amount_mid_default')


# ─────────────────────────────────────────────────────────────────────
# Invoice classifier
# ─────────────────────────────────────────────────────────────────────

def classify_invoice(
    line_items=None,
    total_cents=0,
    status=None,
    linked_job_category=None,
):
    """
    Classify an invoice as treatment/inspection/canceled/unknown by summing
    line-item $ into category buckets and picking the majority.

    Args:
      line_items: list of {name, amount_cents}
      total_cents: invoice total (fallback when items don't match)
      status: invoice status (skip canceled/voided)
      linked_job_category: work_category of the job this invoice is tied to

    Returns: same result dict as classify_job()
    """
    total_cents = total_cents or 0

    # Skip canceled/voided invoices
    if status and status.lower() in ('canceled', 'voided'):
        return _result('canceled', signal='status_canceled')

    # No line items → fall back to linked job, then amount
    if not line_items:
        if linked_job_category in ('treatment', 'inspection'):
            return _result(linked_job_category, signal='linked_job_fallback')
        if total_cents < SMALL_JOB_CENTS:
            return _result('inspection', signal='no_items_small_amount')
        if total_cents == 0:
            return _result('unknown', review_needed=True,
                           review_reason='zero_amount_no_items',
                           signal='zero_amount')
        return _result('treatment', signal='no_items_default_treatment')

    # Sum line items by category
    treatment_cents = 0
    inspection_cents = 0
    unknown_cents = 0
    for item in line_items:
        name = item.get('name') or ''
        amt = item.get('amount_cents') or 0
        cat = _classify_line_item(name)
        if cat == 'treatment':
            treatment_cents += amt
        elif cat == 'inspection':
            inspection_cents += amt
        else:
            unknown_cents += amt

    # Majority wins (by $)
    if treatment_cents > inspection_cents and treatment_cents > 0:
        return _result('treatment', signal='line_items_treatment_majority')
    if inspection_cents > treatment_cents and inspection_cents > 0:
        return _result('inspection', signal='line_items_inspection_majority')

    # Tie (or all line items unknown) — use linked job category, then amount
    if linked_job_category in ('treatment', 'inspection'):
        return _result(linked_job_category, signal='linked_job_tiebreak')
    if total_cents >= SMALL_JOB_CENTS:
        return _result('treatment', signal='amount_tiebreak_mid')
    if total_cents > 0:
        return _result('inspection', signal='amount_tiebreak_small')
    return _result('unknown', review_needed=True,
                   review_reason='no_signals_line_items_ambiguous',
                   signal='ambiguous_no_amount')
