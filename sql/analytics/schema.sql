-- Analytics zone: de-identified data only. Applied by the processor on every start; re-runnable.
--
-- Base tables are written by the processor and readable only by it. Analysts get the v_* views,
-- which show only patients who currently consent to research use.

create table if not exists patients_deid (
    patient_token  text primary key,
    birth_year     integer,            -- null when 90 or older
    age_90_plus    boolean,
    sex            text,
    state          char(2),
    zip3           char(3),
    created_at     timestamptz,        -- shifted
    source_lsn     bigint not null
);

create table if not exists encounters_deid (
    encounter_token  text primary key,
    patient_token    text not null,
    encounter_type   text not null,
    department       text not null,
    admitted_at      timestamptz,      -- shifted
    discharged_at    timestamptz,      -- shifted by the same per-patient offset
    primary_dx_code  text,
    status           text not null,
    source_lsn       bigint not null
);
create index if not exists encounters_deid_patient on encounters_deid (patient_token);

create table if not exists labs_deid (
    lab_token        text primary key,
    encounter_token  text not null,
    patient_token    text not null,
    loinc_code       text not null,
    test_name        text not null,
    value_numeric    numeric(10, 3),
    unit             text,
    reference_low    numeric(10, 3),
    reference_high   numeric(10, 3),
    abnormal_flag    text,
    collected_at     timestamptz,      -- shifted
    resulted_at      timestamptz,      -- shifted
    source_lsn       bigint not null
);
create index if not exists labs_deid_patient on labs_deid (patient_token);

-- Current research consent per patient (no timestamps: when someone changed consent is personal)
create table if not exists consent_state (
    patient_token  text primary key,
    research_use   boolean not null,
    source_lsn     bigint not null
);

-- Rows deleted at the source. A delayed or replayed older event must never bring them back.
create table if not exists deleted_rows (
    table_name  text not null,
    row_token   text not null,
    deleted_lsn bigint not null,
    primary key (table_name, row_token)
);

-- Every privacy-relevant action, without PHI. Restricted like the base tables.
create table if not exists audit_log (
    audit_id       bigint generated always as identity primary key,
    logged_at      timestamptz not null default now(),
    action         text not null,
    patient_token  text,
    table_name     text,
    details        jsonb not null default '{}'
);
create index if not exists audit_log_action on audit_log (action, logged_at);

create table if not exists processor_batches (
    batch_at        timestamptz primary key,
    events          integer not null,
    applied         integer not null,
    stale_skipped   integer not null,
    held_no_consent integer not null,
    errors          integer not null,
    seconds         numeric not null
);

-- ---------------------------------------------------------------- analyst views (consent-gated)

create or replace view v_patients as
select p.patient_token, p.birth_year, p.age_90_plus, p.sex, p.state, p.zip3
from patients_deid as p
join consent_state as c using (patient_token)
where c.research_use;

create or replace view v_encounters as
select e.encounter_token, e.patient_token, e.encounter_type, e.department, e.admitted_at,
       e.discharged_at, e.primary_dx_code, e.status
from encounters_deid as e
join consent_state as c using (patient_token)
where c.research_use;

create or replace view v_labs as
select l.lab_token, l.encounter_token, l.patient_token, l.loinc_code, l.test_name,
       l.value_numeric, l.unit, l.reference_low, l.reference_high, l.abnormal_flag,
       l.collected_at, l.resulted_at
from labs_deid as l
join consent_state as c using (patient_token)
where c.research_use;

-- Interval metrics are exact after per-patient date shifting. Calendar-date metrics are not:
-- each patient's dates move by a different amount, so "admissions per calendar day" is noise.

-- 30-day readmission rate after an inpatient stay, by the department of the first stay
create or replace view v_readmissions_30d as
with index_stays as (
    select patient_token, encounter_token, department, discharged_at
    from v_encounters
    where encounter_type = 'inpatient' and status = 'discharged'
)
select
    s.department,
    count(*) as discharges,
    count(*) filter (where exists (
        select 1 from v_encounters as r
        where r.patient_token = s.patient_token
          and r.encounter_type = 'inpatient'
          and r.status <> 'cancelled'
          and r.admitted_at > s.discharged_at
          and r.admitted_at <= s.discharged_at + interval '30 days'
    )) as readmitted_30d,
    round(avg((exists (
        select 1 from v_encounters as r
        where r.patient_token = s.patient_token
          and r.encounter_type = 'inpatient'
          and r.status <> 'cancelled'
          and r.admitted_at > s.discharged_at
          and r.admitted_at <= s.discharged_at + interval '30 days'
    ))::int)::numeric, 3) as readmission_rate
from index_stays as s
group by s.department;

create or replace view v_length_of_stay as
select
    department,
    encounter_type,
    count(*) as stays,
    round((avg(extract(epoch from discharged_at - admitted_at)) / 86400)::numeric, 2) as avg_days,
    round((percentile_cont(0.5) within group (
        order by extract(epoch from discharged_at - admitted_at)) / 86400)::numeric, 2) as median_days,
    round((percentile_cont(0.9) within group (
        order by extract(epoch from discharged_at - admitted_at)) / 86400)::numeric, 2) as p90_days
from v_encounters
where status = 'discharged'
group by department, encounter_type;

create or replace view v_abnormal_labs as
select
    test_name,
    loinc_code,
    count(*) as results,
    round(avg((abnormal_flag = 'H')::int)::numeric, 3) as share_high,
    round(avg((abnormal_flag = 'L')::int)::numeric, 3) as share_low
from v_labs
group by test_name, loinc_code;
