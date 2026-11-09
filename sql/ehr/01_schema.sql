-- Simulated hospital EHR (the PHI zone). All data in it is synthetic.
-- Runs once when the ehr-db container is first created.

create table patients (
    patient_id     bigint generated always as identity primary key,
    mrn            text not null unique,              -- medical record number
    first_name     text not null,
    last_name      text not null,
    date_of_birth  date not null,
    sex            text not null check (sex in ('F', 'M', 'X', 'U')),
    ssn            text,                              -- synthetic: 9xx area numbers are never issued
    phone          text,                              -- synthetic: 555-01xx is reserved for fiction
    email          text,
    street         text,
    city           text,
    state          char(2),
    zip            char(5),
    created_at     timestamptz not null default now(),
    updated_at     timestamptz not null default now()
);

create table encounters (
    encounter_id     bigint generated always as identity primary key,
    patient_id       bigint not null references patients on delete cascade,
    encounter_type   text not null check (encounter_type in ('inpatient', 'emergency', 'outpatient')),
    department       text not null,
    admitted_at      timestamptz not null,
    discharged_at    timestamptz,
    primary_dx_code  text,                            -- ICD-10-CM
    status           text not null check (status in ('admitted', 'discharged', 'cancelled')),
    attending_npi    text,                            -- provider identifier
    notes            text,                            -- free text: may contain anything
    updated_at       timestamptz not null default now(),
    check (discharged_at is null or discharged_at >= admitted_at)
);
create index on encounters (patient_id);

create table lab_results (
    lab_id          bigint generated always as identity primary key,
    encounter_id    bigint not null references encounters on delete cascade,
    patient_id      bigint not null references patients on delete cascade,
    loinc_code      text not null,
    test_name       text not null,
    value_numeric   numeric(10, 3),
    unit            text,
    reference_low   numeric(10, 3),
    reference_high  numeric(10, 3),
    abnormal_flag   text check (abnormal_flag in ('L', 'N', 'H')),
    collected_at    timestamptz not null,
    resulted_at     timestamptz
);
create index on lab_results (encounter_id);

-- Whether the patient allows their (de-identified) data to be used for research
create table consents (
    patient_id    bigint primary key references patients on delete cascade,
    research_use  boolean not null,
    updated_at    timestamptz not null default now()
);

create function touch_updated_at() returns trigger language plpgsql as $$
begin
    new.updated_at := now();
    return new;
end $$;

create trigger patients_touch before update on patients
    for each row execute function touch_updated_at();
create trigger encounters_touch before update on encounters
    for each row execute function touch_updated_at();
create trigger consents_touch before update on consents
    for each row execute function touch_updated_at();

-- CDC needs the full previous row on updates and deletes, not just the primary key
alter table patients    replica identity full;
alter table encounters  replica identity full;
alter table lab_results replica identity full;
alter table consents    replica identity full;

-- Exactly the tables Debezium may read
create publication ehr_cdc for table patients, encounters, lab_results, consents;
