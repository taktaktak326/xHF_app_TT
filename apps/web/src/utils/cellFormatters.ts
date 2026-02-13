import { createElement, Fragment } from 'react';
import type { ReactNode } from 'react';
import type { CropSeason, CountryCropGrowthStagePrediction } from '../types/farm';
import { getCurrentLanguage, tr } from '../i18n/runtime';
import { getLocalDateString, formatInclusiveEndDate, groupConsecutiveItems } from './formatters';

const STATUS_PRIORITY = ['HIGH', 'MEDIUM_HIGH', 'MEDIUM', 'MEDIUM_LOW', 'LOW', 'PROTECTED'];
const normalizeStatus = (status: string | null | undefined) => (status ?? '').toUpperCase();
const statusSeverity = (status: string | null | undefined) => {
  const normalized = normalizeStatus(status);
  const idx = STATUS_PRIORITY.indexOf(normalized);
  return idx === -1 ? STATUS_PRIORITY.length : idx;
};

export type StatusDisplay = {
  text: string;
  status: string | null;
  prefix?: string;
  range?: string;
};

export const formatCropEstablishmentStage = (season: CropSeason | null): string => {
  const stage = season?.cropEstablishmentGrowthStageIndex;
  return stage ? `BBCH${stage}` : 'N/A';
};

export const formatCropEstablishmentMethod = (season: CropSeason | null): string => {
  const method = season?.cropEstablishmentMethodCode;
  if (!method) return 'N/A';
  if (method === 'TRANSPLANTING') return tr('fmt.crop_method.transplanting');
  if (method === 'DIRECT_SEEDING') return tr('fmt.crop_method.direct_seeding');
  if (method === 'MYKOS_DRY_DIRECT_SEEDING') return tr('fmt.crop_method.mykos_dry_direct_seeding');
  return method;
};

export const formatActiveGrowthStage = (season: CropSeason | null): string => {
  const stage = season?.activeGrowthStage;
  if (!stage) return 'N/A';

  const prefix = stage.scale ? `${stage.scale}` : '';
  const index = stage.index ?? '';

  if (!prefix && !index) return 'N/A';
  return `${prefix}${index}`.trim();
};

/**
 * 作付情報のテキストを生成します。
 */
export const formatCropEstablishmentInfo = (season: CropSeason | null): string => {
  const stageText = formatCropEstablishmentStage(season);
  const methodText = formatCropEstablishmentMethod(season);

  if (stageText === 'N/A' && methodText === 'N/A') return 'N/A';

  const parts: string[] = [];
  if (stageText !== 'N/A') {
    parts.push(`${tr('fmt.crop_stage')}: ${stageText}`);
  }
  if (methodText !== 'N/A') {
    parts.push(`${tr('fmt.crop_method')}: ${methodText}`);
  }
  return parts.join(', ');
};

/**
 * 次の生育ステージのテキストを生成します。
 */
export const formatNextStageInfo = (nextStage: CountryCropGrowthStagePrediction | null): string => {
  if (!nextStage) return 'N/A';
  const stagePrefix = `BBCH${nextStage.gsOrder}`;
  const lang = getCurrentLanguage();
  const dateSuffix = lang === 'ja' ? '〜' : '';
  return `${stagePrefix} : ${nextStage.cropGrowthStageV2?.name ?? 'Unknown Stage'} (${getLocalDateString(nextStage.startDate)}${dateSuffix})`;
};

const translateStatusType = (type: string): string => {
  if (type === 'DISEASE') return tr('fmt.status_type.disease');
  if (type === 'INSECT') return tr('fmt.status_type.insect');
  return type;
};

/**
 * 作期ステータスのテキストを生成します。
 */
export const formatCropSeasonStatus = (season: CropSeason | null): StatusDisplay => {
  if (!season?.cropSeasonStatus || season.cropSeasonStatus.length === 0) {
    return { text: 'N/A', status: null };
  }

  const sorted = [...season.cropSeasonStatus]
    .filter(s => s?.status)
    .sort((a, b) => {
      const sev = statusSeverity(a.status) - statusSeverity(b.status);
      if (sev !== 0) return sev;
      return new Date(a.startDate).getTime() - new Date(b.startDate).getTime();
    });

  const target = sorted[0];
  if (!target) return { text: 'N/A', status: null };

  const prefixRaw = target.type ?? '';
  const prefix = prefixRaw ? translateStatusType(prefixRaw) : '';
  const status = normalizeStatus(target.status);
  const hasDates = target.startDate && target.endDate;
  const range = hasDates ? `(${getLocalDateString(target.startDate)} - ${formatInclusiveEndDate(target.endDate)})` : '';

  return {
    text: `${prefix ? `${prefix}: ` : ''}${status}${range ? ` ${range}` : ''}`,
    status,
    prefix: prefix || undefined,
    range: range || undefined,
  };
};

/**
 * リスクアラートのテキストを生成します。
 */
export const formatRiskAlert = (season: CropSeason | null): StatusDisplay => {
  if (!season?.risks || season.risks.length === 0) return { text: 'N/A', status: null };

  const flattened = season.risks
    .filter(risk => risk?.status)
    .map(risk => {
      const stressInfo = season.timingStressesInfo?.find(info => info.stressV2.uuid === risk.stressV2.uuid);
      return {
        ...risk,
        name: stressInfo?.stressV2.name || 'Unknown Risk',
        groupKey: `${risk.stressV2.uuid}-${normalizeStatus(risk.status)}`,
      };
    });

  const grouped = groupConsecutiveItems(flattened, 'groupKey');

  const sorted = grouped.sort((a, b) => {
    const sev = statusSeverity(a.status) - statusSeverity(b.status);
    if (sev !== 0) return sev;
    return new Date(a.startDate).getTime() - new Date(b.startDate).getTime();
  });

  const target = sorted[0];
  if (!target) return { text: 'N/A', status: null };

  const status = normalizeStatus(target.status);
  const hasDates = target.startDate && target.endDate;
  const range = hasDates ? `${getLocalDateString(target.startDate)} - ${formatInclusiveEndDate(target.endDate)}` : '';

  return {
    text: `${target.name} (${status}${range ? `, ${range}` : ''})`,
    status,
    prefix: target.name,
    range: range ? `(${range})` : undefined,
  };
};

/**
 * 現在アクティブな水管理推奨のテキストを生成します。
 */
export const formatCurrentWaterRecommendations = (recommendations: CropSeason['waterRecommendations']): ReactNode => {
  if (!recommendations) return 'N/A';

  const recommendationNodes = groupConsecutiveItems(
    recommendations.filter(rec => rec.status !== 'INACTIVE' && rec.status !== 'NOT_NEEDED'),
    'actionType',
  ).map((rec, recIndex) => {
    const description = rec.description || rec.actionType;
    const dateRange =
      rec.startDate && rec.endDate
        ? `(${getLocalDateString(rec.startDate)} - ${formatInclusiveEndDate(rec.endDate)})`
        : '';
    const text = `${description}${dateRange ? ` ${dateRange}` : ''}`;
    const lang = getCurrentLanguage();
    const sentences = (lang === 'ja' ? text.split('。') : text.split('.')).filter(s => s);
    const sentenceElements = sentences.map((sentence, sentenceIndex) =>
      createElement(
        'div',
        { key: sentenceIndex },
        `${sentence.trim()}${sentenceIndex < sentences.length - 1 ? (lang === 'ja' ? '。' : '.') : ''}`
      )
    );
    return createElement(Fragment, { key: recIndex }, sentenceElements);
  });

  if (recommendationNodes.length === 0) return 'N/A';
  return recommendationNodes;
};

/**
 * 各種推奨（施肥、水管理、雑草管理）のテキストを生成します。
 */
export const formatRecommendations = (recommendations: CropSeason['nutritionRecommendations'] | CropSeason['waterRecommendations'] | CropSeason['weedManagementRecommendations']): string => {
  if (!recommendations) return 'N/A';

  return groupConsecutiveItems(
    recommendations.filter(rec => rec.status !== 'INACTIVE' && rec.status !== 'NOT_NEEDED'),
    'actionType',
  )
    .map(rec => {
      const label = rec.description || rec.actionType || tr('fmt.recommendation');
      const dateRange =
        rec.startDate && rec.endDate
          ? `${getLocalDateString(rec.startDate)} - ${formatInclusiveEndDate(rec.endDate)}`
          : '';
      return `${label} (${rec.status ?? 'ACTIVE'}${dateRange ? `, ${dateRange}` : ''})`;
    })
    .join('; ') || 'N/A';
};
