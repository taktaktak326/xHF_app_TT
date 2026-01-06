import { withApiBase } from './apiBase';

/**
 * Creates a URL to the backend's download proxy endpoint.
 * @param externalUrl The external URL of the file to download.
 * @param fileName The desired filename for the downloaded file.
 * @returns A URL string pointing to the download endpoint.
 */
export function createDownloadUrl(externalUrl: string, fileName?: string | null): string {
  return withApiBase(`/download-attachment?url=${encodeURIComponent(externalUrl)}&filename=${encodeURIComponent(fileName || 'download')}`);
}
