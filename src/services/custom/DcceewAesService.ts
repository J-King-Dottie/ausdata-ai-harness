import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

import logger from '../../utils/logger.js';
import { DataFlow, DataQueryOptions, DataStructureMetadata, ResolvedDataset } from '../../types/abs.js';

const execFileAsync = promisify(execFile);
const DOWNLOAD_TIMEOUT_MS = 45000;
const PARSE_TIMEOUT_MS = 120000;

interface CustomSheetGroup {
    id: string;
    description: string;
    sheets: string[];
}

interface CustomCurationConfig {
    ignoredSheets?: string[];
    sheetGroups?: CustomSheetGroup[];
}

export class DcceewAesService {
    private readonly parserScriptPath: string;

    constructor(projectRoot: string) {
        this.parserScriptPath = path.join(projectRoot, 'scripts', 'dcceew_aes_xlsx.py');
    }

    supports(flow: DataFlow): boolean {
        return flow.flowType === 'dcceew_aes_xlsx';
    }

    async getMetadata(flow: DataFlow): Promise<DataStructureMetadata> {
        const workbookPath = await this.downloadWorkbook(flow);
        try {
            return await this.runMetadata(flow, workbookPath);
        } finally {
            await this.safeUnlink(workbookPath);
        }
    }

    async query(flow: DataFlow, dataKey: string = 'all', options?: DataQueryOptions): Promise<ResolvedDataset> {
        const workbookPath = await this.downloadWorkbook(flow);
        try {
            return await this.runResolve(flow, workbookPath, dataKey, options);
        } finally {
            await this.safeUnlink(workbookPath);
        }
    }

    async resolve(flow: DataFlow, options: DataQueryOptions & { dataKey?: string }): Promise<ResolvedDataset> {
        const dataKey = String(options.dataKey ?? '').trim() || 'all';
        return this.query(flow, dataKey, options);
    }

    private async runMetadata(flow: DataFlow, workbookPath: string): Promise<DataStructureMetadata> {
        const { stdout } = await execFileAsync('python3', [
            this.parserScriptPath,
            'metadata',
            '--xlsx',
            workbookPath,
            '--dataset-id',
            flow.id,
            '--agency-id',
            flow.agencyID,
            '--version',
            flow.version,
            '--name',
            flow.name,
            '--description',
            flow.description,
            '--curation-json',
            JSON.stringify(flow.curation ?? {}),
        ], {
            cwd: path.dirname(this.parserScriptPath),
            maxBuffer: 4 * 1024 * 1024,
            timeout: PARSE_TIMEOUT_MS,
        });

        return JSON.parse(stdout) as DataStructureMetadata;
    }

    private async runResolve(
        flow: DataFlow,
        workbookPath: string,
        dataKey: string,
        options?: DataQueryOptions,
    ): Promise<ResolvedDataset> {
        const { stdout } = await execFileAsync('python3', [
            this.parserScriptPath,
            'resolve',
            '--xlsx',
            workbookPath,
            '--dataset-id',
            flow.id,
            '--agency-id',
            flow.agencyID,
            '--version',
            flow.version,
            '--name',
            flow.name,
            '--description',
            flow.description,
            '--data-key',
            dataKey,
            '--detail',
            options?.detail ?? 'full',
            '--curation-json',
            JSON.stringify(flow.curation ?? {}),
        ], {
            cwd: path.dirname(this.parserScriptPath),
            maxBuffer: 16 * 1024 * 1024,
            timeout: PARSE_TIMEOUT_MS,
        });

        return JSON.parse(stdout) as ResolvedDataset;
    }

    private async downloadWorkbook(flow: DataFlow): Promise<string> {
        if (!flow.sourceUrl) {
            throw new Error(`Custom flow ${flow.id} is missing sourceUrl`);
        }

        const tmpPath = path.join(
            os.tmpdir(),
            `${flow.id.toLowerCase()}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}.xlsx`,
        );
        logger.info('Starting DCCEEW AES workbook download', {
            datasetId: flow.id,
            sourceUrl: flow.sourceUrl,
        });
        try {
            await execFileAsync('curl', [
                '-sSL',
                '--max-time',
                String(Math.ceil(DOWNLOAD_TIMEOUT_MS / 1000)),
                flow.sourceUrl,
                '-o',
                tmpPath,
            ], {
                cwd: path.dirname(this.parserScriptPath),
                maxBuffer: 1024 * 1024,
                timeout: DOWNLOAD_TIMEOUT_MS + 5000,
            });
        } catch (error) {
            logger.error('DCCEEW AES workbook download failed', {
                datasetId: flow.id,
                sourceUrl: flow.sourceUrl,
                error,
            });
            throw new Error(
                `Timed out downloading live DCCEEW workbook for ${flow.id} from ${flow.sourceUrl}.`,
            );
        }
        logger.info('Downloaded DCCEEW AES workbook', {
            datasetId: flow.id,
            sourceUrl: flow.sourceUrl,
            tmpPath,
        });
        return tmpPath;
    }

    private async safeUnlink(filePath: string): Promise<void> {
        try {
            await fs.unlink(filePath);
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
                logger.warn('Failed to remove temporary workbook', { filePath, error });
            }
        }
    }
}
