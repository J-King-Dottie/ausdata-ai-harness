import logger from '../../utils/logger.js';
import { ABSApiClient } from './ABSApiClient.js';
import { DataFlowService } from './DataFlowService.js';
import { DcceewAesService } from '../custom/DcceewAesService.js';
import {
    DataQueryOptions,
    DataFlow,
    DimensionValueSummary,
    ResolvedDataset,
    CondensedSeries,
    CondensedObservation
} from '../../types/abs.js';

interface ResolveDatasetOptions extends DataQueryOptions {
    datasetId: string;
    dataKey?: string;
    forceRefresh?: boolean;
}

interface QueryContext {
    dataKey: string;
    startPeriod?: string;
    endPeriod?: string;
    detail: NonNullable<DataQueryOptions['detail']>;
    dimensionAtObservation?: string;
}

export class DatasetResolver {
    private readonly apiClient: ABSApiClient;
    private readonly dcceewAesService: DcceewAesService;

    constructor(private readonly dataFlowService: DataFlowService) {
        this.apiClient = new ABSApiClient();
        this.dcceewAesService = new DcceewAesService(process.cwd());
    }

    async resolve(options: ResolveDatasetOptions): Promise<ResolvedDataset> {
        const {
            datasetId,
            dataKey,
            startPeriod,
            endPeriod,
            detail,
            dimensionAtObservation,
            forceRefresh,
            format
        } = options;

        const targetFormat: DataQueryOptions['format'] = format ?? 'jsondata';
        if (targetFormat !== 'jsondata') {
            throw new Error(`resolve_dataset currently supports format=jsondata (received ${targetFormat})`);
        }
        const resolvedDetail: NonNullable<DataQueryOptions['detail']> = detail ?? 'full';
        if (resolvedDetail !== 'full') {
            logger.info('resolve_dataset applying non-default detail level', { datasetId, resolvedDetail });
        }

        const normalizedId = datasetId;
        const flow = await this.dataFlowService.resolveFlow(normalizedId, forceRefresh ?? false);
        if (this.dcceewAesService.supports(flow)) {
            return this.dcceewAesService.resolve(flow, {
                dataKey,
                startPeriod,
                endPeriod,
                detail: resolvedDetail,
                dimensionAtObservation,
                format: targetFormat,
            });
        }

        const dataflowIdentifier = DataFlowService.formatDataflowIdentifier(flow);
        const queryDataKey = dataKey && dataKey.trim().length > 0 ? dataKey.trim() : 'all';
        const queryDimensionAtObservation = dimensionAtObservation ?? 'TIME_PERIOD';

        logger.info('Resolving dataset', {
            datasetId: dataflowIdentifier,
            dataKey: queryDataKey,
            startPeriod,
            endPeriod,
            detail: resolvedDetail,
            dimensionAtObservation: queryDimensionAtObservation
        });

        const datasetResponse = await this.apiClient.getData(dataflowIdentifier, queryDataKey, {
            format: targetFormat,
            startPeriod,
            endPeriod,
            detail: resolvedDetail,
            dimensionAtObservation: queryDimensionAtObservation
        });

        const resolved = this.transformJsonData(flow, {
            dataKey: queryDataKey,
            startPeriod,
            endPeriod,
            detail: resolvedDetail,
            dimensionAtObservation: queryDimensionAtObservation
        }, datasetResponse);

        return resolved;
    }

    private transformJsonData(
        flow: DataFlow,
        query: QueryContext,
        payload: any
    ): ResolvedDataset {
        if (Array.isArray(payload?.errors) && payload.errors.length > 0) {
            throw new Error(`ABS API returned errors: ${JSON.stringify(payload.errors)}`);
        }

        const dataEnvelope = payload?.data;
        if (!dataEnvelope) {
            throw new Error('ABS response did not include a data section.');
        }

        const structure = this.toArray(dataEnvelope.structures)[0];
        if (!structure) {
            throw new Error('ABS response did not include structure metadata.');
        }

        const seriesDimensions = this.toArray(structure.dimensions?.series);
        const observationDimensions = this.toArray(structure.dimensions?.observation);
        const seriesAttributeDefs = this.toArray(structure.attributes?.series);
        const observationAttributeDefs = this.toArray(structure.attributes?.observation);

        const dataset = (this.toArray(dataEnvelope.dataSets)[0] ?? {}) as any;
        if (!dataset) {
            throw new Error('ABS response did not include dataset observations.');
        }

        const dimensionMap = this.buildDimensionLookup([
            ...seriesDimensions,
            ...observationDimensions
        ]);

        const seriesGroups = new Map<string, CondensedSeries>();
        const ensureSeriesGroup = (seriesKey: string): CondensedSeries => {
            let group = seriesGroups.get(seriesKey);
            if (!group) {
                group = { seriesKey, observations: [] };
                seriesGroups.set(seriesKey, group);
            }
            return group;
        };

        let observationCount = 0;
        const seriesEntries = dataset.series ? Object.entries<any>(dataset.series) : [];
        if (seriesEntries.length > 0) {
            for (const [seriesKey, seriesEntry] of seriesEntries) {
                const seriesIndexValues = this.parseKeyIndices(seriesKey, seriesDimensions.length);
                const seriesCoordinates = this.buildCoordinateRecord(seriesDimensions, seriesIndexValues);
                const condensedSeriesCoordinates = this.toCondensedCoordinateMap(seriesCoordinates);

                const seriesAttributeValues = this.mapAttributeValues(
                    seriesAttributeDefs,
                    Array.isArray(seriesEntry?.attributes) ? seriesEntry.attributes : []
                );
                const compactSeriesAttributes = this.compactAttributes(seriesAttributeValues);

                const group = ensureSeriesGroup(seriesKey);
                if (Object.keys(condensedSeriesCoordinates).length > 0) {
                    group.dimensions = condensedSeriesCoordinates;
                }
                if (compactSeriesAttributes) {
                    group.attributes = compactSeriesAttributes;
                }

                const observationEntries = Object.entries<any>(seriesEntry?.observations ?? {});
                for (const [observationKey, valueArray] of observationEntries) {
                    const observationIndices = this.parseKeyIndices(observationKey, observationDimensions.length);
                    const observationCoordinates = this.buildCoordinateRecord(observationDimensions, observationIndices);
                    const condensedObservationCoordinates = this.toCondensedCoordinateMap(observationCoordinates);

                    const observationAttributeValues = this.mapAttributeValues(
                        observationAttributeDefs,
                        Array.isArray(valueArray) ? valueArray.slice(1) : []
                    );
                    const compactObservationAttributes = this.compactAttributes(observationAttributeValues);

                    const resolvedObservation: CondensedObservation = {
                        observationKey,
                        value: this.coerceValue(Array.isArray(valueArray) ? valueArray[0] : valueArray)
                    };
                    if (Object.keys(condensedObservationCoordinates).length > 0) {
                        resolvedObservation.dimensions = condensedObservationCoordinates;
                    }
                    if (compactObservationAttributes) {
                        resolvedObservation.attributes = compactObservationAttributes;
                    }
                    group.observations.push(resolvedObservation);
                    observationCount += 1;
                }
            }
        } else {
            const fallbackKey = '__all__';
            const group = ensureSeriesGroup(fallbackKey);
            const observationEntries = Object.entries<any>(dataset.observations ?? {});
            for (const [observationKey, valueArray] of observationEntries) {
                const indices = this.parseKeyIndices(observationKey, observationDimensions.length);
                const observationCoordinates = this.buildCoordinateRecord(observationDimensions, indices);
                const condensedObservationCoordinates = this.toCondensedCoordinateMap(observationCoordinates);

                const observationAttributeValues = this.mapAttributeValues(
                    observationAttributeDefs,
                    Array.isArray(valueArray) ? valueArray.slice(1) : []
                );
                const compactObservationAttributes = this.compactAttributes(observationAttributeValues);

                const resolvedObservation: CondensedObservation = {
                    observationKey,
                    value: this.coerceValue(Array.isArray(valueArray) ? valueArray[0] : valueArray)
                };
                if (Object.keys(condensedObservationCoordinates).length > 0) {
                    resolvedObservation.dimensions = condensedObservationCoordinates;
                }
                if (compactObservationAttributes) {
                    resolvedObservation.attributes = compactObservationAttributes;
                }
                group.observations.push(resolvedObservation);
                observationCount += 1;
            }
        }

        const series: CondensedSeries[] = Array.from(seriesGroups.values())
            .map((group) => {
                if (!group.dimensions || Object.keys(group.dimensions).length === 0) {
                    delete group.dimensions;
                }
                if (!group.attributes || Object.keys(group.attributes).length === 0) {
                    delete group.attributes;
                }
                group.observations = group.observations.map((obs) => {
                    if (!obs.dimensions || Object.keys(obs.dimensions).length === 0) {
                        delete obs.dimensions;
                    }
                    if (!obs.attributes || Object.keys(obs.attributes).length === 0) {
                        delete obs.attributes;
                    }
                    return obs;
                });
                return group;
            })
            .sort((a, b) => a.seriesKey.localeCompare(b.seriesKey));

        const dimensionLookup = Object.fromEntries(
            Object.entries(dimensionMap).filter(([, values]) => Object.keys(values).length > 0)
        );

        return {
            dataset: {
                id: flow.id,
                agencyID: flow.agencyID,
                version: flow.version,
                name: flow.name,
                description: flow.description
            },
            query: this.buildQuerySummary(query),
            dimensions: dimensionLookup,
            observationCount,
            series
        };
    }

    private buildQuerySummary(query: QueryContext): ResolvedDataset['query'] {
        const summary: ResolvedDataset['query'] = {
            dataKey: query.dataKey
        };
        summary.detail = query.detail;
        if (query.startPeriod) {
            summary.startPeriod = query.startPeriod;
        }
        if (query.endPeriod) {
            summary.endPeriod = query.endPeriod;
        }
        if (query.dimensionAtObservation) {
            summary.dimensionAtObservation = query.dimensionAtObservation;
        }
        return summary;
    }

    private buildDimensionLookup(dimensions: any[]): Record<string, Record<string, string>> {
        const lookup: Record<string, Record<string, string>> = {};
        dimensions.forEach((dim) => {
            const dimensionId = dim?.id;
            if (!dimensionId) {
                return;
            }
            const values = this.toArray(dim?.values);
            if (values.length === 0) {
                return;
            }
            const registry = (lookup[dimensionId] = lookup[dimensionId] ?? {});
            values.forEach((value: any) => {
                const code = value?.id;
                if (!code || registry[code]) {
                    return;
                }
                const label = this.extractName(value);
                registry[code] = label && label.length > 0 ? label : code;
            });
        });
        return lookup;
    }

    private buildCoordinateRecord(dimensions: any[], indices: number[]): Record<string, DimensionValueSummary> {
        const record: Record<string, DimensionValueSummary> = {};
        dimensions.forEach((dim, idx) => {
            const index = indices[idx] ?? 0;
            const dimensionId = dim?.id ?? `DIM_${idx}`;
            const valueMeta = this.toArray(dim?.values)[index];
            record[dimensionId] = {
                code: valueMeta?.id ?? String(index),
                label: this.extractName(valueMeta),
                description: this.extractDescription(valueMeta)
            };
        });
        return record;
    }

    private toCondensedCoordinateMap(
        coordinates: Record<string, DimensionValueSummary>
    ): Record<string, { code: string; label?: string }> {
        const result: Record<string, { code: string; label?: string }> = {};
        Object.entries(coordinates).forEach(([dimensionId, valueSummary]) => {
            if (!valueSummary) {
                return;
            }
            const entry: { code: string; label?: string } = {
                code: valueSummary.code ?? ''
            };
            if (valueSummary.label && valueSummary.label.length > 0 && valueSummary.label !== valueSummary.code) {
                entry.label = valueSummary.label;
            }
            result[dimensionId] = entry;
        });
        return result;
    }

    private mapAttributeValues(definitions: any[], values: any[]): Record<string, string | number | null> {
        const result: Record<string, string | number | null> = {};
        definitions.forEach((def, index) => {
            const value = values?.[index];
            if (value === null || value === undefined || value === '') {
                return;
            }
            const mapped = this.lookupValue(def?.values, value);
            result[def?.id ?? `ATTR_${index}`] = mapped;
        });
        return result;
    }

    private lookupValue(options: any[], value: any): string | number {
        if (!options || options.length === 0) {
            return typeof value === 'object' ? JSON.stringify(value) : value;
        }

        const valueAsString = String(value);
        const matchById = options.find((option: any) => option?.id === valueAsString);
        if (matchById) {
            return this.extractName(matchById) ?? valueAsString;
        }

        const numericIndex = Number(value);
        if (!Number.isNaN(numericIndex) && options[numericIndex]) {
            return this.extractName(options[numericIndex]) ?? value;
        }

        return value;
    }

    private coerceValue(value: any): number | string | null {
        if (value === null || value === undefined) {
            return null;
        }
        if (typeof value === 'number') {
            return value;
        }
        if (typeof value === 'string') {
            const numeric = Number(value);
            return Number.isNaN(numeric) ? value : numeric;
        }
        return value;
    }

    private parseKeyIndices(key: string, expectedLength: number): number[] {
        if (expectedLength === 0) {
            return [];
        }
        const parts = typeof key === 'string' ? key.split(':') : [];
        const indices = parts
            .filter((part) => part !== '')
            .map((part) => {
                const numeric = Number(part);
                return Number.isNaN(numeric) ? 0 : numeric;
            });
        while (indices.length < expectedLength) {
            indices.push(0);
        }
        return indices.slice(0, expectedLength);
    }

    private compactAttributes(
        attributes: Record<string, string | number | null>
    ): Record<string, string | number | null> | undefined {
        const entries = Object.entries(attributes).filter(
            ([, value]) => value !== null && value !== undefined && value !== ''
        );
        if (entries.length === 0) {
            return undefined;
        }
        return Object.fromEntries(entries);
    }

    private extractName(value: any): string | undefined {
        if (value === null || value === undefined) {
            return undefined;
        }
        if (typeof value === 'string') {
            return value;
        }
        if (typeof value === 'number') {
            return value.toString();
        }
        if (typeof value.name === 'string') {
            return value.name;
        }
        if (value.name && typeof value.name === 'object') {
            return value.name.en ?? Object.values(value.name)[0];
        }
        if (value.names && typeof value.names === 'object') {
            return value.names.en ?? Object.values(value.names)[0];
        }
        if (typeof value.label === 'string') {
            return value.label;
        }
        return value.id ?? undefined;
    }

    private extractDescription(value: any): string | undefined {
        if (!value) {
            return undefined;
        }
        if (typeof value.description === 'string') {
            return value.description;
        }
        if (value.descriptions && typeof value.descriptions === 'object') {
            return value.descriptions.en ?? Object.values(value.descriptions)[0];
        }
        return undefined;
    }

    private toArray<T>(value: T | T[] | undefined | null): T[] {
        if (!value) {
            return [];
        }
        return Array.isArray(value) ? value : [value];
    }
}
