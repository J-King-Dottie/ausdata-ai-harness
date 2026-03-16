import fs from 'fs/promises';
import path from 'path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import logger from '../../utils/logger.js';
import { ABSApiClient } from './ABSApiClient.js';
import {
    DataFlow,
    DataFlowCache,
    DataQueryOptions,
    DataStructureMetadata,
    DimensionMetadata,
    AttributeMetadata,
    CodeListMetadata,
    CodeItem,
    ConceptMetadata,
    DataStructureReference
} from '../../types/abs.js';

const execFileAsync = promisify(execFile);

export class DataFlowService {
    private cache: DataFlowCache | null = null;
    private readonly cacheFilePath: string;
    private readonly refreshIntervalMs: number;
    private readonly apiClient: ABSApiClient;
    private readonly ftsDbPath: string;
    private readonly ftsScriptPath: string;

    constructor(cacheFilePath: string, refreshIntervalHours: number = 24) {
        const resolvedCachePath = path.resolve(cacheFilePath);
        this.cacheFilePath = resolvedCachePath;
        this.refreshIntervalMs = refreshIntervalHours * 60 * 60 * 1000;
        this.apiClient = new ABSApiClient();
        this.ftsDbPath = path.join(path.dirname(resolvedCachePath), 'ABS_DATAFLOWS_FTS.sqlite3');
        this.ftsScriptPath = path.join(path.dirname(resolvedCachePath), 'scripts', 'abs_dataflows_fts.py');

        logger.info('DataFlowService initialized', {
            cacheFilePath: this.cacheFilePath,
            refreshIntervalHours,
            refreshIntervalMs: this.refreshIntervalMs,
            ftsDbPath: this.ftsDbPath,
            ftsScriptPath: this.ftsScriptPath
        });
    }

    async getDataFlows(forceRefresh: boolean = false): Promise<DataFlow[]> {
        logger.debug('Getting data flows', { forceRefresh });
        try {
            if (!this.cache) {
                logger.info('Cache not initialized, attempting to load from file');
                this.cache = await this.loadCache();
            }

            if (forceRefresh || !this.isCacheValid()) {
                logger.info('Cache invalid or refresh forced, fetching new data');
                const flows = await this.fetchDataFlows();
                this.cache = {
                    lastUpdated: new Date(),
                    flows
                };
                await this.saveCache(this.cache);
                return flows;
            }

            logger.debug('Returning cached data flows', {
                flowCount: this.cache?.flows.length ?? 0,
                cacheAge: this.cache ? new Date().getTime() - new Date(this.cache.lastUpdated).getTime() : 0
            });
            return this.cache?.flows ?? [];

        } catch (error) {
            logger.error('Error getting data flows', { error });
            throw error;
        }
    }

    async getFlowData(flowId: string, dataKey: string = 'all', options?: DataQueryOptions) {
        logger.info('Getting flow data', { flowId, dataKey, options });
        return this.apiClient.getData(flowId, dataKey, options);
    }

    async resolveFlow(
        dataflowIdentifier: string,
        forceRefresh: boolean = false
    ): Promise<DataFlow> {
        logger.debug('Resolving dataflow identifier', { dataflowIdentifier, forceRefresh });

        const { agencyId, dataflowId, version } = DataFlowService.parseDataflowIdentifier(dataflowIdentifier);
        const flows = await this.getDataFlows(forceRefresh);

        const candidates = flows.filter(
            (flow) =>
                flow.id === dataflowId &&
                (agencyId ? flow.agencyID === agencyId : true)
        );

        if (candidates.length === 0) {
            throw new Error(`Unknown dataflow identifier: ${dataflowIdentifier}`);
        }

        if (version) {
            const match = candidates.find((flow) => flow.version === version);
            if (!match) {
                throw new Error(`Dataflow ${dataflowIdentifier} not found for version ${version}`);
            }
            return match;
        }

        const latest = DataFlowService.selectLatestFlow(candidates);
        if (!latest) {
            throw new Error(`Unable to resolve latest version for dataflow ${dataflowIdentifier}`);
        }

        return latest;
    }

    async getDataStructureForDataflow(
        dataflowIdentifier: string,
        forceRefresh: boolean = false
    ): Promise<DataStructureMetadata> {
        logger.info('Fetching data structure for dataflow', { dataflowIdentifier, forceRefresh });

        const { agencyId, dataflowId, version } = DataFlowService.parseDataflowIdentifier(dataflowIdentifier);
        const flows = await this.getDataFlows(forceRefresh);

        const candidates = flows.filter(
            (flow) =>
                flow.id === dataflowId &&
                (agencyId ? flow.agencyID === agencyId : true)
        );

        if (candidates.length === 0) {
            throw new Error(`Unknown dataflow identifier: ${dataflowIdentifier}`);
        }

        const selectedFlow = version
            ? candidates.find((flow) => flow.version === version)
            : DataFlowService.selectLatestFlow(candidates);

        if (!selectedFlow) {
            throw new Error(`No matching version found for dataflow ${dataflowIdentifier}`);
        }

        const structureRef = selectedFlow.structure ?? {
            id: selectedFlow.id,
            agencyID: selectedFlow.agencyID,
            version: selectedFlow.version
        };

        const structure = await this.apiClient.getDataStructure(
            structureRef.agencyID ?? selectedFlow.agencyID,
            structureRef.id,
            structureRef.version ?? selectedFlow.version,
            'children',
            'full'
        );

        const metadata = this.extractDataStructure(structure);
        metadata.dataflow = selectedFlow;
        return metadata;
    }

    private async fetchDataFlows(): Promise<DataFlow[]> {
        logger.info('Fetching data flows');
        try {
            const parsed = await this.apiClient.getDataFlows('ABS');
            return this.extractDataFlows(parsed);
        } catch (error) {
            logger.error('Error fetching data flows', { error });
            throw error;
        }
    }

    private extractDataFlows(parsed: any): DataFlow[] {
        logger.debug('Extracting data flows from parsed XML');
        try {
            const dataflows =
                parsed.Structure?.Structures?.Dataflows?.Dataflow ??
                parsed.Structure?.Dataflows?.Dataflow ??
                [];

            const flows = this.toArray(dataflows);

            return flows.map((flow: any) => {
                const dataFlow: DataFlow = {
                    id: flow.id,
                    agencyID: flow.agencyID,
                    version: flow.version,
                    name: this.extractText(flow.Name),
                    description: this.extractText(flow.Description)
                };

                // Add structure reference if available
                if (flow.Structure?.Ref) {
                    dataFlow.structure = {
                        id: flow.Structure.Ref.id,
                        version: flow.Structure.Ref.version,
                        agencyID: flow.Structure.Ref.agencyID
                    };
                }

                return dataFlow;
            });
        } catch (error) {
            logger.error('Error extracting data flows from parsed XML', { error });
            throw error;
        }
    }

    async searchDataFlows(
        query: string,
        limit: number = 8,
        forceRefresh: boolean = false
    ): Promise<DataFlow[]> {
        if (forceRefresh) {
            await this.getDataFlows(true);
        }

        const normalizedQuery = this.normalizeSearchText(query);
        if (!normalizedQuery) {
            const flows = await this.getDataFlows(forceRefresh);
            return flows.slice(0, Math.max(1, limit));
        }

        const { stdout } = await execFileAsync('python3', [
            this.ftsScriptPath,
            '--json-cache',
            this.cacheFilePath,
            '--db',
            this.ftsDbPath,
            '--query',
            query,
            '--limit',
            String(Math.max(1, limit))
        ], {
            cwd: path.dirname(this.cacheFilePath),
            maxBuffer: 1024 * 1024
        });

        const parsed = JSON.parse(stdout) as { dataflows?: DataFlow[] };
        if (!Array.isArray(parsed.dataflows)) {
            throw new Error('FTS search returned an invalid payload');
        }

        return parsed.dataflows;
    }

    private extractDataStructure(parsed: any): DataStructureMetadata {
        const structures = parsed.Structure?.Structures ?? {};
        const dataStructureNode = this.first(
            structures.DataStructures?.DataStructure
        );

        if (!dataStructureNode) {
            throw new Error('No data structure found in ABS response');
        }

        const dimensions = this.extractDimensions(
            dataStructureNode.DataStructureComponents?.DimensionList?.Dimension
        );
        const attributes = this.extractAttributes(
            dataStructureNode.DataStructureComponents?.AttributeList?.Attribute
        );
        const codelists = this.extractCodelists(structures.Codelists?.Codelist);
        const concepts = this.extractConcepts(structures.Concepts?.ConceptScheme);

        return {
            dataStructure: {
                id: dataStructureNode.id,
                agencyID: dataStructureNode.agencyID,
                version: dataStructureNode.version,
                name: this.extractText(dataStructureNode.Name),
                description: this.extractText(dataStructureNode.Description)
            },
            dimensions,
            attributes,
            codelists,
            concepts
        };
    }

    private extractDimensions(dimensionNode: any): DimensionMetadata[] {
        const dimensions = this.toArray(dimensionNode);
        return dimensions.map((dimension: any, index: number) => {
            const conceptRef = dimension.ConceptIdentity?.Ref;
            const enumRef =
                dimension.LocalRepresentation?.Enumeration?.Ref ??
                dimension.Representation?.Enumeration?.Ref;

            const position = dimension.position
                ? Number(dimension.position)
                : index + 1;

            const codelistRef = enumRef
                ? ({
                      id: enumRef.id,
                      agencyID: enumRef.agencyID,
                      version: enumRef.version
                  } as DataStructureReference)
                : undefined;

            return {
                id: dimension.id,
                position: Number.isNaN(position) ? undefined : position,
                conceptId: conceptRef?.id,
                role: dimension.Role?.Ref?.id,
                codelist: codelistRef
            };
        });
    }

    private extractAttributes(attributeNode: any): AttributeMetadata[] {
        const attributes = this.toArray(attributeNode);
        return attributes.map((attribute: any) => {
            const conceptRef = attribute.ConceptIdentity?.Ref;
            const enumRef =
                attribute.LocalRepresentation?.Enumeration?.Ref ??
                attribute.Representation?.Enumeration?.Ref;

            const attachmentLevel = typeof attribute.AttachmentLevel === 'string'
                ? attribute.AttachmentLevel
                : typeof attribute.attachmentLevel === 'string'
                ? attribute.attachmentLevel
                : undefined;

            const relatedTo = this.extractAttributeRelationship(attribute.AttributeRelationship);

            return {
                id: attribute.id,
                assignmentStatus: attribute.assignmentStatus,
                attachmentLevel,
                conceptId: conceptRef?.id,
                codelist: enumRef
                    ? ({
                          id: enumRef.id,
                          agencyID: enumRef.agencyID,
                          version: enumRef.version
                      } as DataStructureReference)
                    : undefined,
                relatedTo: relatedTo.length ? relatedTo : undefined
            };
        });
    }

    private extractAttributeRelationship(relationshipNode: any): string[] {
        if (!relationshipNode) {
            return [];
        }

        const related = new Set<string>();

        const dimensionRefs = this.toArray(relationshipNode.Dimension);
        dimensionRefs.forEach((dimension: any) => {
            const id = dimension.Ref?.id ?? dimension.id;
            if (id) {
                related.add(id);
            }
        });

        const groupRefs = this.toArray(relationshipNode.Group);
        groupRefs.forEach((group: any) => {
            const id = group.Ref?.id ?? group.id;
            if (id) {
                related.add(id);
            }
        });

        const primaryMeasure = relationshipNode.PrimaryMeasure?.Ref?.id;
        if (primaryMeasure) {
            related.add(primaryMeasure);
        }

        const observation = relationshipNode.Observation;
        if (observation) {
            related.add('OBSERVATION');
        }

        return Array.from(related);
    }

    private extractCodelists(codelistNode: any): CodeListMetadata[] {
        const codelists = this.toArray(codelistNode);
        return codelists.map((codelist: any) => {
            const codes = this.toArray(codelist.Code).map((code: any) => {
                const parentId = code.Parent?.Ref?.id ?? code.ParentID;
                return {
                    id: code.id,
                    name: this.extractText(code.Name),
                    description: this.extractText(code.Description),
                    parentID: parentId
                } as CodeItem;
            });

            return {
                id: codelist.id,
                agencyID: codelist.agencyID,
                version: codelist.version,
                name: this.extractText(codelist.Name),
                description: this.extractText(codelist.Description),
                codes
            };
        });
    }

    private extractConcepts(conceptSchemeNode: any): ConceptMetadata[] {
        const schemes = this.toArray(conceptSchemeNode);
        const concepts: ConceptMetadata[] = [];

        schemes.forEach((scheme: any) => {
            const schemeInfo = {
                id: scheme.id,
                agencyID: scheme.agencyID,
                version: scheme.version,
                name: this.extractText(scheme.Name)
            };

            this.toArray(scheme.Concept).forEach((concept: any) => {
                concepts.push({
                    id: concept.id,
                    name: this.extractText(concept.Name),
                    description: this.extractText(concept.Description),
                    scheme: schemeInfo
                });
            });
        });

        return concepts;
    }

    private async loadCache(): Promise<DataFlowCache | null> {
        logger.debug('Loading cache from file', { path: this.cacheFilePath });
        try {
            const data = await fs.readFile(this.cacheFilePath, 'utf8');
            const parsed = JSON.parse(data) as Partial<DataFlowCache> & {
                dataflows?: DataFlow[];
            };

            if (!Array.isArray(parsed.flows)) {
                if (Array.isArray(parsed.dataflows)) {
                    logger.info('Legacy cache format detected; ignoring cached dataflows');
                    return null;
                }
                logger.warn('Cache file does not contain a flows array; ignoring cache file');
                return null;
            }

            const cache: DataFlowCache = {
                lastUpdated: parsed.lastUpdated
                    ? new Date(parsed.lastUpdated)
                    : new Date(0),
                flows: parsed.flows,
            };
            logger.info('Successfully loaded cache', {
                flowCount: cache.flows.length,
                lastUpdated: cache.lastUpdated
            });
            return cache;
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
                logger.info('No cache file found', { path: this.cacheFilePath });
                return null;
            }
            logger.error('Error loading cache', { error });
            throw error;
        }
    }

    private async saveCache(cache: DataFlowCache): Promise<void> {
        logger.debug('Saving cache to file', { 
            path: this.cacheFilePath,
            flowCount: cache.flows.length 
        });
        try {
            await fs.mkdir(path.dirname(this.cacheFilePath), { recursive: true });
            await fs.writeFile(this.cacheFilePath, JSON.stringify(cache, null, 2));
            logger.info('Successfully saved cache');
        } catch (error) {
            logger.error('Error saving cache', { error });
            throw error;
        }
    }

    private isCacheValid(): boolean {
        if (!this.cache) {
            logger.debug('Cache is null');
            return false;
        }

        const age = new Date().getTime() - new Date(this.cache.lastUpdated).getTime();
        const isValid = age < this.refreshIntervalMs;
        
        logger.debug('Checking cache validity', {
            age,
            refreshIntervalMs: this.refreshIntervalMs,
            isValid
        });
        
        return isValid;
    }

    // Utility method to format a dataflow identifier for use in data queries
    public static formatDataflowIdentifier(flow: DataFlow): string {
        return `${flow.agencyID},${flow.id},${flow.version}`;
    }

    private normalizeSearchText(value: string): string {
        return String(value || '')
            .toLowerCase()
            .replace(/[^a-z0-9]+/g, ' ')
            .replace(/\s+/g, ' ')
            .trim();
    }

    private tokenizeSearchText(value: string): string[] {
        const stopwords = new Set([
            'the', 'and', 'for', 'with', 'from', 'that', 'this', 'into',
            'over', 'under', 'using', 'show', 'data', 'series', 'table',
            'tables', 'latest', 'time', 'timeseries', 'trend', 'what',
            'which', 'where'
        ]);
        return this.normalizeSearchText(value)
            .split(' ')
            .filter((token) => token.length > 1 && !stopwords.has(token));
    }

    public static parseDataflowIdentifier(identifier: string): {
        agencyId: string;
        dataflowId: string;
        version?: string;
    } {
        let candidate = identifier.trim();

        if (candidate.startsWith('{') && candidate.includes('datasetId')) {
            try {
                const parsed = JSON.parse(candidate);
                if (typeof parsed.datasetId === 'string') {
                    candidate = parsed.datasetId.trim();
                }
            } catch (error) {
                logger.warn('Failed to parse datasetId JSON wrapper', { identifier, error });
            }
        }

        const parts = candidate.split(',').map((part) => part.trim()).filter(Boolean);

        if (parts.length === 0) {
            throw new Error('Empty dataflow identifier provided');
        }

        if (parts.length === 1) {
            return {
                agencyId: 'ABS',
                dataflowId: parts[0]
            };
        }

        if (parts.length === 2) {
            return {
                agencyId: parts[0] || 'ABS',
                dataflowId: parts[1]
            };
        }

        return {
            agencyId: parts[0] || 'ABS',
            dataflowId: parts[1],
            version: parts[2]
        };
    }

    private static selectLatestFlow(flows: DataFlow[]): DataFlow | null {
        if (flows.length === 0) {
            return null;
        }

        return flows.reduce((latest, current) => {
            return DataFlowService.compareVersions(current.version, latest.version) > 0
                ? current
                : latest;
        }, flows[0]);
    }

    private static compareVersions(a: string, b: string): number {
        const aParts = a.split('.').map((part) => parseInt(part, 10));
        const bParts = b.split('.').map((part) => parseInt(part, 10));
        const length = Math.max(aParts.length, bParts.length);

        for (let i = 0; i < length; i++) {
            const aValue = aParts[i] ?? 0;
            const bValue = bParts[i] ?? 0;

            if (aValue > bValue) {
                return 1;
            }
            if (aValue < bValue) {
                return -1;
            }
        }

        return 0;
    }

    private extractText(value: any): string {
        if (value === undefined || value === null) {
            return '';
        }

        if (typeof value === 'string') {
            return value;
        }

        if (Array.isArray(value)) {
            const preferred =
                value.find((entry) => entry?.lang === 'en') ?? value[0];
            return this.extractText(preferred);
        }

        if (typeof value === 'object' && value._text !== undefined) {
            return this.extractText(value._text);
        }

        return '';
    }

    private toArray<T>(value: T | T[] | undefined | null): T[] {
        if (!value) {
            return [];
        }
        return Array.isArray(value) ? value : [value];
    }

    private first<T>(value: T | T[] | undefined | null): T | undefined {
        if (!value) {
            return undefined;
        }
        return Array.isArray(value) ? value[0] : value;
    }
}
