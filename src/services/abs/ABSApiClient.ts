import axios, { AxiosInstance } from 'axios';
import { XMLParser } from 'fast-xml-parser';
import logger from '../../utils/logger.js';
import { DetailLevel, ReferenceScope, DataFormat, DataQueryOptions, ABSError } from '../../types/abs.js';

export class ABSApiClient {
    private readonly api: AxiosInstance;
    private readonly xmlParser: XMLParser;

    constructor() {
        this.api = axios.create({
            baseURL: 'https://data.api.abs.gov.au',
            timeout: 120000, // 120 seconds to accommodate large SDMX payloads
            headers: {
                'Accept': 'application/xml'
            }
        });

        this.xmlParser = new XMLParser({
            ignoreAttributes: false,
            attributeNamePrefix: '',
            textNodeName: '_text',
            ignoreDeclaration: true,
            removeNSPrefix: true
        });

        // Add response interceptor for logging
        this.api.interceptors.response.use(
            (response) => {
                logger.debug('API Response received', {
                    url: response.config.url,
                    status: response.status,
                    dataSize: response.data?.length
                });
                return response;
            },
            (error) => {
                this.handleError(error);
                throw error;
            }
        );
    }

    async getDataFlows(agencyId: string = 'ABS', detail?: DetailLevel) {
        logger.info('Fetching dataflows from ABS API', { agencyId, detail });

        const response = await this.api.get(`/rest/dataflow/${agencyId}`, {
            params: detail ? { detail } : undefined,
            headers: {
                'Accept': 'application/vnd.sdmx.structure+xml;version=2.1'
            }
        });

        return this.xmlParser.parse(response.data);
    }

    async getStructures(
        structureType: string,
        agencyId: string = 'ABS',
        detail?: DetailLevel,
        references?: ReferenceScope
    ) {
        logger.info('Fetching structures from ABS API', {
            structureType,
            agencyId,
            detail,
            references
        });

        const response = await this.api.get(`/rest/${structureType}/${agencyId}`, {
            params: {
                detail,
                references
            }
        });

        return this.xmlParser.parse(response.data);
    }

    async getDataStructure(
        agencyId: string,
        structureId: string,
        version?: string,
        references: ReferenceScope = 'children',
        detail: DetailLevel = 'full'
    ) {
        logger.info('Fetching data structure from ABS API', {
            agencyId,
            structureId,
            version,
            references,
            detail
        });

        const versionPath = version ? `/${version}` : '';
        const response = await this.api.get(
            `/rest/datastructure/${agencyId}/${structureId}${versionPath}`,
            {
                params: {
                    references,
                    detail
                },
                headers: {
                    Accept: 'application/vnd.sdmx.structure+xml;version=2.1'
                }
            }
        );

        return this.xmlParser.parse(response.data);
    }

    async getData(
        dataflowId: string,
        dataKey: string = 'all',
        options?: DataQueryOptions
    ) {
        const format = options?.format ?? 'jsondata';

        const params: Record<string, string> = {};
        if (options?.startPeriod) params.startPeriod = options.startPeriod;
        if (options?.endPeriod) params.endPeriod = options.endPeriod;
        if (options?.detail) params.detail = options.detail;
        if (options?.dimensionAtObservation) {
            params.dimensionAtObservation = options.dimensionAtObservation;
        }
        if (typeof options?.lastNObservations === 'number') {
            params.lastNObservations = String(options.lastNObservations);
        }
        if (typeof options?.firstNObservations === 'number') {
            params.firstNObservations = String(options.firstNObservations);
        }
        params.format = format;

        const requestPath = `/rest/data/${dataflowId}/${dataKey}`;
        const query = new URLSearchParams(params).toString();
        const requestUrl = query
            ? `https://data.api.abs.gov.au${requestPath}?${query}`
            : `https://data.api.abs.gov.au${requestPath}`;

        logger.info('Fetching data from ABS API', {
            dataflowId,
            dataKey,
            options,
            requestUrl
        });

        const response = await this.api.get(requestPath, {
            params,
            headers: {
                Accept: this.getAcceptHeader(format)
            }
        });

        if (format === 'jsondata') {
            const payload = response.data;
            if (typeof payload === 'string') {
                try {
                    return JSON.parse(payload);
                } catch (error) {
                    logger.warn('Failed to parse ABS jsondata response as JSON; returning raw string', {
                        dataflowId,
                        dataKey,
                        error
                    });
                    return payload;
                }
            }
            return payload;
        }

        if (format.startsWith('csv')) {
            return response.data;
        }

        return this.xmlParser.parse(response.data);
    }

    private getAcceptHeader(format?: DataFormat): string {
        switch (format) {
            case 'csvfile':
            case 'csvfilewithlabels':
                return 'text/csv';
            case 'jsondata':
                return 'application/vnd.sdmx.data+json';
            case 'genericdata':
                return 'application/xml';
            case 'structurespecificdata':
                return 'application/vnd.sdmx.structurespecificdata+xml';
            default:
                return 'application/xml';
        }
    }

    private handleError(error: any): never {
        const absError: ABSError = new Error('ABS API Error');
        
        if (axios.isAxiosError(error)) {
            absError.message = error.message;
            absError.status = error.response?.status;
            absError.statusText = error.response?.statusText;
            absError.url = error.config?.url;

            logger.error('ABS API Error', {
                status: error.response?.status,
                statusText: error.response?.statusText,
                url: error.config?.url,
                message: error.message
            });
        } else {
            absError.message = error.message || 'Unknown error';
            logger.error('Unknown API Error', { error });
        }

        throw absError;
    }
}
