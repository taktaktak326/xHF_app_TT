import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import { Doughnut, Bar } from 'react-chartjs-2';
import { Chart as ChartJS, ArcElement, Tooltip, Legend, CategoryScale, LinearScale, BarElement, TimeScale, TimeSeriesScale, TooltipItem, ChartType, Plugin, ChartEvent, ActiveElement } from 'chart.js';
import 'chartjs-adapter-date-fns';



interface GrowthStageTimelineProps {
  items: any[];
  workMap: Map<string, Map<string, WorkPeriod[]>>;
  earliestDate: string | null;
  verticalLineLabel: string | null;
  chartRef: React.RefObject<ChartJS<'bar', any, any>>;
  onBarClick: (data: { bbchIndex: string; fields?: any[]; stageName?: string }) => void;
  selectedBbchs: string[];
  isGrouped: boolean;
}

const GrowthStageTimeline: React.FC<GrowthStageTimelineProps> = React.memo(({ items, workMap, earliestDate, verticalLineLabel, chartRef, onBarClick, selectedBbchs, isGrouped }) => {
    const { chartData, chartHeight, maxDate } = useTimelineData(items, workMap, selectedBbchs, isGrouped);
    
    if (!chartData || chartData.datasets.length === 0) {
        return <div className="chart-no-data">表示できる生育予測データがありません。生育予測対象外の作物、または生育予測機能が割り当てられていません。</div>;
    }

    const today = new Date();
    const todayForTimeline = startOfDay(today);
    const initialViewMin = subDays(todayForTimeline, 14).getTime();
    const initialViewMax = addDays(todayForTimeline, 14).getTime();
    
    const limitMin = subDays(today, 100).getTime();
    const limitMax = maxDate ? maxDate.getTime() : addDays(today, 180).getTime();

    const options = {
        indexAxis: 'y' as const,
        responsive: true,
        maintainAspectRatio: false,
        layout: {
            padding: {
                top: 30,
                bottom: 30
            }
        },
        onClick: (evt: ChartEvent, elements: ActiveElement[], chart: ChartJS) => {
            if (elements.length > 0) {
                const element = elements[0];
                const dataset = chart.data.datasets[element.datasetIndex];
                const rawData = dataset.data[element.index] as any;
                if (dataset.bbchIndex) {
                    onBarClick({
                        bbchIndex: dataset.bbchIndex,
                        fields: rawData.containedFields,
                        stageName: rawData.stageName
                    });
                }
            }
        },
        plugins: {
            legend: {
                display: false,
            },
            tooltip: {
                callbacks: {
                    title: (tooltipItems: TooltipItem<'bar'>[]): string => {
                         if (tooltipItems.length > 0) {
                            const label = tooltipItems[0].label;
                            return Array.isArray(label) ? label.join(', ') : label;
                        }
                        return '';
                    },
                    label: (context: TooltipItem<'bar'>): string => {
                        const rawData = context.raw as { x: [number, number], stageName?: string };
                        const bbchIndex = context.dataset.bbchIndex || '';
                        const label = `BBCH ${bbchIndex}`;
                        if (rawData && rawData.x) {
                            const start = format(new Date(rawData.x[0]), 'MM/dd');
                            const end = format(new Date(rawData.x[1]), 'MM/dd');
                            const prefix = isGrouped ? '平均 ' : '';
                            return `${prefix}${label} [${start} - ${end}]`;
                        }
                        return label;
                    },
                    footer: (tooltipItems: TooltipItem<'bar'>[]): string | string[] => {
                        if (tooltipItems.length === 0) return '';
                        const tooltipItem = tooltipItems[0];
                        const rawData = tooltipItem.raw as any;
                        const lines: (string|null)[] = [];
                        
                        if (rawData && rawData.x) {
                            const start = new Date(rawData.x[0]);
                            const end = new Date(rawData.x[1]);
                            const duration = differenceInCalendarDays(end, start) + 1;
                             lines.push(`${isGrouped ? '平均期間' : '期間'}: ${duration}日`);
                        }

                        if (rawData.workPeriods && rawData.workPeriods.length > 0) {
                            lines.push(''); 
                            lines.push('関連作業:');
                            const uniqueWorkNames = new Set<string>();
                            rawData.workPeriods.forEach((work: any) => {
                                if (!uniqueWorkNames.has(work.name)) {
                                    lines.push(`${workCategories[work.category].icon} ${work.name}`);
                                    uniqueWorkNames.add(work.name);
                                }
                            });
                        }
                        
                        if (rawData.containedFields && rawData.containedFields.length > 0) {
                            lines.push('');
                            lines.push(`対象圃場 (${rawData.containedFields.length}件):`);
                            const fieldNames = rawData.containedFields.slice(0, 3).map((f: any) => f.name).join(', ');
                            const moreCount = rawData.containedFields.length - 3;
                            lines.push(fieldNames + (moreCount > 0 ? ` 他${moreCount}件` : ''));
                            if (isGrouped) {
                                lines.push('(クリックして全て表示)');
                            }
                        }

                        return lines.filter(l => l !== null) as string[];
                    }
                }
            },
            datalabels: {
                labels: {
                    bbch: {
                        clip: true,
                        // @ts-ignore
                        display: (context: any) => {
                            const value = context.dataset.data[context.dataIndex];
                            if (!value || !value.x) return false;
                            const scale = context.chart.scales.x;
                            const [start, end] = value.x;
                            if (end < scale.min || start > scale.max) return false;
                            const visibleStart = Math.max(start, scale.min);
                            const visibleEnd = Math.min(end, scale.max);
                            const pixelWidth = scale.getPixelForValue(visibleEnd) - scale.getPixelForValue(visibleStart);
                            return pixelWidth > 20;
                        },
                        formatter: (_value: any, context: any) => context.dataset.bbchIndex,
                        color: (context: any) => getTextColorForBg(context.dataset.backgroundColor as string),
                        font: { weight: 'bold', size: 12 },
                        anchor: 'center' as const, align: 'center' as const,
                    },
                    workIcons: {
                        display: true, clip: true,
                        formatter: (value: any) => {
                            const work = value.workPeriods;
                            if (work && work.length > 0) {
                                const icons = new Set(work.map((w: any) => workCategories[w.category].icon));
                                return Array.from(icons).join(' ');
                            }
                            return null;
                        },
                        color: '#000', font: { size: 14, weight: 'bold' },
                        align: 'top' as const, anchor: 'middle' as const,
                        offset: 2, textStrokeColor: 'white', textStrokeWidth: 2,
                    }
                }
            },
            verticalLine: {
                earliestDate: earliestDate,
                labelText: verticalLineLabel,
                today: new Date().toISOString(),
                todayLabel: '今日'
            },
            zoom: {
                pan: { enabled: true, mode: 'x' as const },
                zoom: {
                    wheel: { enabled: true },
                    pinch: { enabled: true },
                    mode: 'x' as const,
                },
                limits: {
                    x: { min: limitMin, max: limitMax, minRange: 7 * 24 * 60 * 60 * 1000 }
                }
            }
        },
        scales: {
            x: {
                type: 'time' as const, position: 'top' as const,
                min: initialViewMin, max: initialViewMax,
                adapters: { date: { locale: ja } },
                time: {
                    tooltipFormat: 'yyyy/MM/dd', minUnit: 'day',
                    displayFormats: { day: 'M/d', week: 'M/d', month: 'yyyy年 M月', year: 'yyyy年' }
                }
            },
            y: {
                stacked: true,
                ticks: { autoSkip: false }
            }
        },
    };

    return (
        <div className="gantt-scroll-container">
            <div className="gantt-chart-wrapper" style={{ height: `${chartHeight}px` }}>
                <Bar ref={chartRef} data={chartData} options={options as any} />
            </div>
        </div>
    );
});