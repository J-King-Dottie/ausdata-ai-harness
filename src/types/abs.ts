export interface DataFlow {
    id: string;
    agencyID: string;
    version: string;
    name: string;
    description: string;
    flowType?: string;
    sourceType?: string;
    sourceUrl?: string;
    sourcePageUrl?: string;
    sourceOrganization?: string;
    requiresMetadataBeforeRetrieval?: boolean;
    curation?: {
        ignoredSheets?: string[];
        sheetGroups?: Array<{
            id: string;
            description: string;
            sheets: string[];
        }>;
    };
    structure?: {
        id: string;
        version: string;
        agencyID: string;
    };
}

export interface DataFlowCache {
    lastUpdated: Date;
    flows: DataFlow[];
}

export type DetailLevel = 
    | 'full'
    | 'allstubs'
    | 'referencestubs'
    | 'referencepartial'
    | 'allcompletestubs'
    | 'referencecompletestubs';

export type ReferenceScope = 
    | 'none'
    | 'parents'
    | 'parentsandsiblings'
    | 'children'
    | 'descendants'
    | 'all'
    | 'datastructure'
    | 'dataflow'
    | 'codelist'
    | 'conceptscheme'
    | 'categoryscheme'
    | 'contentconstraint'
    | 'actualconstraint'
    | 'agencyscheme'
    | 'categorisation'
    | 'hierarchicalcodelist';

export type DataFormat = 
    | 'csvfilewithlabels'
    | 'csvfile'
    | 'jsondata'
    | 'genericdata'
    | 'structurespecificdata';

export interface DataQueryOptions {
    startPeriod?: string;
    endPeriod?: string;
    format?: DataFormat;
    detail?: 'full' | 'dataonly' | 'serieskeysonly' | 'nodata';
    dimensionAtObservation?: 'TIME_PERIOD' | 'AllDimensions' | string;
    lastNObservations?: number;
    firstNObservations?: number;
}

export interface ABSError extends Error {
    status?: number;
    statusText?: string;
    url?: string;
}

export interface DataStructureReference {
    id: string;
    agencyID?: string;
    version?: string;
}

export interface DimensionMetadata {
    id: string;
    position?: number;
    conceptId?: string;
    role?: string;
    codelist?: DataStructureReference;
}

export interface AttributeMetadata {
    id: string;
    assignmentStatus?: string;
    attachmentLevel?: string;
    conceptId?: string;
    codelist?: DataStructureReference;
    relatedTo?: string[];
}

export interface CodeItem {
    id: string;
    name?: string;
    description?: string;
    parentID?: string;
}

export interface CodeListMetadata {
    id: string;
    agencyID?: string;
    version?: string;
    name?: string;
    description?: string;
    codes: CodeItem[];
}

export interface ConceptMetadata {
    id: string;
    name?: string;
    description?: string;
    scheme?: {
        id?: string;
        agencyID?: string;
        version?: string;
        name?: string;
    };
}

export interface DataStructureMetadata {
    dataStructure: {
        id: string;
        agencyID?: string;
        version?: string;
        name?: string;
        description?: string;
    };
    dataflow?: DataFlow;
    dimensions: DimensionMetadata[];
    attributes: AttributeMetadata[];
    codelists: CodeListMetadata[];
    concepts: ConceptMetadata[];
}

export interface DimensionValueSummary {
    code: string;
    label?: string;
    description?: string;
}

export interface ResolvedDataset {
    dataset: {
        id: string;
        agencyID?: string;
        version?: string;
        name?: string;
        description?: string;
    };
    query: {
        dataKey: string;
        startPeriod?: string;
        endPeriod?: string;
        dimensionAtObservation?: string;
        detail?: 'full' | 'dataonly' | 'serieskeysonly' | 'nodata';
    };
    dimensions: Record<string, Record<string, string>>;
    observationCount: number;
    series: CondensedSeries[];
}

export interface CondensedDimensionValue {
    code: string;
    label?: string;
}

export interface CondensedObservation {
    observationKey: string;
    value: number | string | null;
    dimensions?: Record<string, CondensedDimensionValue>;
    attributes?: Record<string, string | number | null>;
}

export interface CondensedSeries {
    seriesKey: string;
    dimensions?: Record<string, CondensedDimensionValue>;
    attributes?: Record<string, string | number | null>;
    observations: CondensedObservation[];
}

export interface DimensionAvailabilityValue extends DimensionValueSummary {
    seriesCount?: number;
}

export interface DimensionAvailabilitySummary {
    dimensionId: string;
    label?: string;
    description?: string;
    required: boolean;
    supportsMultiSelect: boolean;
    observedValueCount: number;
    totalValueCount?: number;
    sampleSize: number;
    values: DimensionAvailabilityValue[];
    valueLimitHit?: boolean;
    missingCodes?: DimensionAvailabilityValue[];
    missingLimitHit?: boolean;
    fixedValue?: string;
    notes?: string[];
}

export interface AvailabilityGuidance {
    general: string[];
    compatibilityHints: string[];
}

export interface DatasetAvailabilityMap {
    datasetId: string;
    generatedAt: string;
    totalSeries: number;
    dimensionOrder: string[];
    dataflow?: DataFlow;
    dimensionAvailability: Record<string, DimensionAvailabilitySummary>;
    guidance: AvailabilityGuidance;
    fixedDimensions?: Record<string, string>;
}
