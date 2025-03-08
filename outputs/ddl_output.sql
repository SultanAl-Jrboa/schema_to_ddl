-- DDL for database: POSTGRESQL


CREATE TABLE NIC_DWH_STG."cycles" (
    "actualEndDate" timestamp without time zone,
    "actualEndEntityDate" timestamp without time zone,
    "createdAt" timestamp without time zone,
    "endDate" timestamp without time zone,
    "endEntityDate" timestamp without time zone,
    "id" BIGINT,
    "isCurrent" BOOLEAN,
    "isLast" BOOLEAN,
    "month" integer,
    "name" character varying,
    "startDate" timestamp without time zone,
    "updatedAt" timestamp without time zone,
    "year" integer,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    PRIMARY KEY ("id")
);

CREATE TABLE NIC_DWH_STG."entities" (
    "createdAt" timestamp without time zone,
    "id" BIGINT,
    "name" character varying,
    "parentId" BIGINT,
    "updatedAt" timestamp without time zone,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    "etimadId" BIGINT,
    PRIMARY KEY ("id")
);

CREATE TABLE NIC_DWH_STG."entity_sector" (
    "createdAt" timestamp without time zone,
    "id" BIGINT,
    "sectorId" BIGINT,
    "updatedAt" timestamp without time zone,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    "entityId" BIGINT,
    PRIMARY KEY ("id")
);

CREATE TABLE NIC_DWH_STG."execution_plan" (
    "createdAt" timestamp without time zone,
    "id" BIGINT,
    "initiativeCycleId" BIGINT,
    "mainOutputs" character varying,
    "milestoneName" character varying,
    "milestoneStatus" BIGINT,
    "supportDocuments" character varying,
    "targetEndDate" timestamp without time zone,
    "targetStartDate" timestamp without time zone,
    "updatedAt" timestamp without time zone,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    PRIMARY KEY ("id")
);

CREATE TABLE NIC_DWH_STG."financial_savings" (
    "actual" jsonb,
    "actualTotalSummision" double precision,
    "actualYearsSummition" jsonb,
    "createdAt" timestamp without time zone,
    "expected" jsonb,
    "expectedTotalSummision" double precision,
    "expectedYearsSummition" jsonb,
    "finalApprovedCosts" double precision,
    "futureYears" double precision,
    "id" BIGINT,
    "initiativeCycleId" BIGINT,
    "isDeleted" BIGINT,
    "lastUpdate" timestamp without time zone,
    "spendingBeforeInitiative" double precision,
    "targetBudget" jsonb,
    "totalExpectedFinancialImpact" double precision,
    "updatedAt" timestamp without time zone,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    PRIMARY KEY ("id")
);

CREATE TABLE NIC_DWH_STG."initiatives" (
    "createdAt" timestamp without time zone,
    "department" character varying,
    "description" character varying,
    "effectedDescription" character varying,
    "effectedEntities" jsonb,
    "endDate" timestamp without time zone,
    "entityId" BIGINT,
    "id" BIGINT,
    "initiativeDescriptivePlan" character varying,
    "initiativeGoals" character varying,
    "initiativeOwner" character varying,
    "initiativeSerialNumber" BIGINT,
    "initiativeValue" character varying,
    "isDeleted" BIGINT,
    "name" character varying,
    "savesDescription" character varying,
    "savesType" BIGINT,
    "startDate" timestamp without time zone,
    "totalExpectedSaves" double precision,
    "updatedAt" timestamp without time zone,
    "userId" character varying,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    "completionDate" timestamp without time zone,
    "initiativeAffectOnAnotherEntities" BIGINT,
    "initiativeGoalsHasRelationWithSpendingEfficiency" BIGINT,
    "initiativeHasDescriptivePlan" BIGINT,
    "initiativeHasValue" BIGINT,
    "IRONumber" character varying,
    "financialType" BIGINT,
    PRIMARY KEY ("id")
);

CREATE TABLE NIC_DWH_STG."initiatives_cycles" (
    "PlannedAchieveSummary" character varying,
    "achievedSummary" character varying,
    "calcSavingMechanism" character varying,
    "createdAt" timestamp without time zone,
    "cycleId" BIGINT,
    "externalRequiredSupport" character varying,
    "generalComments" character varying,
    "id" BIGINT,
    "initiativeId" BIGINT,
    "requiredSupport" character varying,
    "status" BIGINT,
    "updated" BIGINT,
    "updatedAt" timestamp without time zone,
    "workflowStatus" BIGINT,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    "evaluationFieldsCommentId" BIGINT,
    PRIMARY KEY ("id")
);

CREATE TABLE NIC_DWH_STG."lookup_details" (
    "createdAt" timestamp without time zone,
    "description" character varying,
    "id" BIGINT,
    "lookupDetailNameAr" character varying,
    "lookupDetailNameEn" character varying,
    "lookupMasterId" BIGINT,
    "updatedAt" timestamp(6)without time zone,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    "orderBy" integer,
    PRIMARY KEY ("id")
);

CREATE TABLE NIC_DWH_STG."lookup_master" (
    "createdAt" timestamp(6)without time zone,
    "description" character varying,
    "id" BIGINT,
    "lookupMasterNameAr" character varying,
    "lookupMasterNameEn" character varying,
    "updatedAt" timestamp(6)without time zone,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    PRIMARY KEY ("id")
);

CREATE TABLE NIC_DWH_STG."sectors" (
    "createdAt" timestamp(6)without time zone,
    "id" BIGINT,
    "sectorName" character varying,
    "updatedAt" timestamp(6)without time zone,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    PRIMARY KEY ("id")
);

CREATE TABLE NIC_DWH_STG."users" (
    "createdAt" timestamp(6)without time zone,
    "deletedAt" timestamp(6)without time zone,
    "email" character varying,
    "firstName" character varying,
    "isActive" BOOLEAN,
    "lastLogin" timestamp(6)without time zone,
    "lastName" character varying,
    "province" integer,
    "telephoneNumber" character varying,
    "updatedAt" timestamp(6)without time zone,
    "userId" character varying,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    PRIMARY KEY ("userId")
);

CREATE TABLE NIC_DWH_STG."users_entities" (
    "entitiesId" BIGINT,
    "usersUserId" character varying,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    PRIMARY KEY ("entitiesId", "usersUserId")
);

CREATE TABLE NIC_DWH_STG."users_sectors" (
    "sectorsId" BIGINT,
    "usersUserId" character varying,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    PRIMARY KEY ("sectorsId", "usersUserId")
);

CREATE TABLE NIC_DWH_STG."entity_base_line" (
    "createdAt" timestamp(6)without time zone,
    "updatedAt" timestamp without time zone,
    "id" BIGINT,
    "year" BIGINT,
    "entityId" BIGINT,
    "entityBudget" double precision,
    "deletedAt" timestamp without time zone,
    "deletedBy" character varying,
    "updatedBy" character varying,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    PRIMARY KEY ("id")
);

CREATE TABLE NIC_DWH_STG."evaluations" (
    "createdAt" timestamp(6)without time zone,
    "updatedAt" timestamp without time zone,
    "id" BIGINT,
    "userId" character varying,
    "isDeleted" BIGINT,
    "initiativeCycleId" BIGINT,
    "initiativeClarityEvaluation" jsonb,
    "completionSpeed" jsonb,
    "initiativeClarityEvaluationValue" integer,
    "completionSpeedValue" integer,
    "entityId" BIGINT,
    "cycleId" BIGINT,
    "fieldsComments" jsonb,
    "initiativeGeneralComment" character varying,
    "returnComment" character varying,
    "deletedAt" timestamp without time zone,
    "deletedBy" character varying,
    "updatedBy" character varying,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    PRIMARY KEY ("id")
);

CREATE TABLE NIC_DWH_STG."change_requests" (
    "createdAt" timestamp(6)without time zone,
    "updatedAt" timestamp without time zone,
    "id" BIGINT,
    "initiativeId" BIGINT,
    "entityId" BIGINT,
    "userId" character varying,
    "dataBeforeChangeRequest" jsonb,
    "dataAfterChangeRequest" jsonb,
    "status" BIGINT,
    "cycleId" BIGINT,
    "changeRequestType" BIGINT,
    "deletedAt" timestamp without time zone,
    "deletedBy" character varying,
    "updatedBy" character varying,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    PRIMARY KEY ("id")
);

CREATE TABLE NIC_DWH_STG."entiteis_evaluation_notes" (
    "createdAt" timestamp(6)without time zone,
    "updatedAt" timestamp without time zone,
    "id" BIGINT,
    "initiativeSerialNumber" BIGINT,
    "initiativeCycleId" BIGINT,
    "notes" character varying,
    "fieldsNotes" jsonb,
    "reflected" integer,
    "deletedAt" timestamp without time zone,
    "deletedBy" character varying,
    "updatedBy" character varying,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    PRIMARY KEY ("id")
);

CREATE TABLE NIC_DWH_STG."entiteis_evaluation_view" (
    "createdAt" timestamp(6)without time zone,
    "updatedAt" timestamp without time zone,
    "id" BIGINT,
    "entityId" BIGINT,
    "cycleId" BIGINT,
    "initiativeClarityAndCalcMechanism" double precision,
    "implementationAndCompletionSpeed" double precision,
    "commitment" double precision,
    "completeInsertion" double precision,
    "initiativeSize" double precision,
    "generalEvaluation" double precision,
    "deletedAt" timestamp without time zone,
    "deletedBy" character varying,
    "updatedBy" character varying,
    "SynchTimestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "LastOperation" character CHECK ("LastOperation" IN ('INSERT', 'UPDATE', 'DELETE')),
    PRIMARY KEY ("id")
);