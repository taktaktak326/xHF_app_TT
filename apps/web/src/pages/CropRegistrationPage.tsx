import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { ChangeEvent, FC, ReactNode } from 'react';
import LoadingOverlay from '../components/LoadingOverlay';
import { useData } from '../context/DataContext';
import { useFarms } from '../context/FarmContext';
import { useAuth } from '../context/AuthContext';
import { withApiBase } from '../utils/apiBase';
import { formatCombinedLoadingMessage } from '../utils/loadingMessage';
import { postJsonCached } from '../utils/cachedJsonFetch';
import './CropRegistrationPage.css';

const CROP_LIST = [
  '稲',
  '大豆',
  '小麦（秋まき）',
  '大麦（秋まき）',
  'キャベツ',
  'たまねぎ',
  'にんじん',
  'ばれいしょ',
  'てんさい',
  'ブロッコリー',
  'とうもろこし',
  '小豆',
  'いんげんまめ',
  'レンゲ',
  'そば',
  'なたね',
  'クリムゾンクローバー',
  'ナヨクサフジ（ヘアリーベッチ）',
  '牧草',
] as const;

const CROP_DATA = {
  LIST: CROP_LIST as unknown as string[],
  ICONS: {
    稲: '🌾',
    大豆: '🌱',
    '小麦（秋まき）': '🌾',
    '大麦（秋まき）': '🌾',
    キャベツ: '🥬',
    たまねぎ: '🧅',
    にんじん: '🥕',
    ばれいしょ: '🥔',
    てんさい: '🍬',
    ブロッコリー: '🥦',
    とうもろこし: '🌽',
    小豆: '🫘',
    いんげんまめ: '🫛',
    レンゲ: '🌸',
    そば: '🍜',
    なたね: '🌼',
    クリムゾンクローバー: '🍀',
    'ナヨクサフジ（ヘアリーベッチ）': '🌿',
    牧草: '🌿',
  } as Record<string, string>,
  TILLAGE_OPTIONS: ['不耕起', '中耕培土', '代かき', '天地返し', '直播', '耕うん・耕起', '耕起', '該当なし'],
  NEEDS_PLANTING_METHOD: ['稲', 'てんさい'],
  PLANTING_METHOD_OPTIONS: {
    稲: ['移植', '湛水直播水稲', '乾田直播水稲', '乾田直播水稲（節水）'],
    てんさい: ['移植', '直播'],
  } as Record<string, string[]>,
  GROWTH_STAGE_OPTIONS: {
    稲: {
      移植: ['BBCH11', 'BBCH12', 'BBCH13', 'BBCH14'],
      湛水直播水稲: ['BBCH0', 'BBCH1', 'BBCH3', 'BBCH5'],
      乾田直播水稲: ['BBCH0', 'BBCH1', 'BBCH3', 'BBCH5'],
      '乾田直播水稲（節水）': ['BBCH0', 'BBCH1', 'BBCH3', 'BBCH5'],
    },
    てんさい: {
      移植: ['BBCH12', 'BBCH14', 'BBCH15', 'BBCH16', 'BBCH17'],
      直播: ['BBCH0'],
    },
    default: ['BBCH0'],
  } as Record<string, any>,
  PREVIOUS_CROP_OPTIONS: [...CROP_LIST, '該当なし'] as string[],
};

const ALLOWED_CROP_NAME_SET = new Set<string>(CROP_LIST);

const needsSeedingTillageSystem = (cropName: string) =>
  cropName !== '稲' && CROP_DATA.NEEDS_PLANTING_METHOD.includes(cropName);

const cropsNoTillage = new Set(['ばれいしょ', 'てんさい', '稲', '大豆']);
const needsTillage = (cropName: string) => Boolean(cropName) && !cropsNoTillage.has(cropName);

type CropOption = {
  uuid: string;
  name: string;
  eppoCode?: string | null;
  scientificName?: string | null;
};

type VarietyOption = {
  uuid: string;
  name: string;
  code?: string;
  registrationNumber?: string;
};

type TillageSystemOption = {
  uuid: string;
  name: string;
  code?: string;
  description?: string | null;
};
type TillageOption = TillageSystemOption;


type FormData = {
  fieldId: string;
  fieldName: string;
  crop_name: string;
  cropUuid: string;
  variety: string;
  varietyUuid: string;
  planting_method: string;
  growth_stage: string;
  planting_date: string;
  yield: string;
  previous_crop: string;
  previousCropUuid: string;
  tillage: string;
  tillageUuid: string;
  prefecture: string;
  municipality: string;
  seedingTillageSystemUuid: string;
  seedingTillageSystemName: string;
};

type CropSeasonCreatePayload = {
  fieldUuid: string;
  cropUuid: string;
  varietyUuid: string;
  startDate: string;
  yieldExpectation: number;
  cropEstablishmentMethodCode?: string | null;
  cropEstablishmentGrowthStageIndex?: string | null;
  tillageUuid?: string | null;
  seedingTillageSystemUuid?: string | null;
  preCropUuid?: string | null;
};

type PaginationResult<T> = {
  paginatedData: T[];
  PaginationControls: FC<{ className?: string }>;
  setCurrentPage: (page: number) => void;
  currentPage: number;
  totalPages: number;
};

type CombinedSeasonResponse = {
  uuid?: string;
  lifecycleState?: string | null;
  crop?: { uuid?: string | null; name?: string | null } | null;
  variety?: { uuid?: string | null; name?: string | null } | null;
  cropEstablishmentMethodCode?: string | null;
  cropEstablishmentGrowthStageIndex?: string | number | null;
  startDate?: string | null;
  preCropUuid?: string | null;
  preCrop?: { uuid?: string | null; name?: string | null } | null;
};

type CombinedFieldResponse = {
  uuid?: string;
  name?: string;
  area?: number | null;
  cropSeasonsV2?: CombinedSeasonResponse[] | null;
};

const usePagination = <T,>(data: T[], itemsPerPage: number): PaginationResult<T> => {
  const [currentPage, setCurrentPage] = useState(1);
  const totalPages = Math.ceil(data.length / itemsPerPage);

  const paginatedData = useMemo(() => {
    const startIndex = (currentPage - 1) * itemsPerPage;
    return data.slice(startIndex, startIndex + itemsPerPage);
  }, [data, currentPage, itemsPerPage]);

  useEffect(() => {
    if (currentPage > totalPages && totalPages > 0) {
      setCurrentPage(totalPages);
    } else if (totalPages === 0 && currentPage !== 1) {
      setCurrentPage(1);
    }
  }, [data.length, totalPages, currentPage]);

  const PaginationControls: FC<{ className?: string }> = ({ className }) =>
    totalPages > 1 ? (
      <div className={`pagination-controls ${className || ''}`}>
        <button onClick={() => setCurrentPage((p) => p - 1)} disabled={currentPage === 1}>
          前へ
        </button>
        <span className="pagination-info">{`${currentPage} / ${totalPages}`}</span>
        <button onClick={() => setCurrentPage((p) => p + 1)} disabled={currentPage === totalPages}>
          次へ
        </button>
      </div>
    ) : null;

  return { paginatedData, PaginationControls, setCurrentPage, currentPage, totalPages };
};

const Modal: FC<{ isOpen: boolean; children: ReactNode; className?: string }> = ({ isOpen, children, className }) => {
  if (!isOpen) return null;
  return (
    <div className={`modal-overlay ${className || ''}`}>
      <div className="modal-content">{children}</div>
    </div>
  );
};

const ConfirmationModal: FC<{
  isOpen: boolean;
  onConfirm: () => void;
  onCancel: () => void;
  title: string;
  message: ReactNode;
}> = ({ isOpen, onConfirm, onCancel, title, message }) => {
  useEffect(() => {
    if (!isOpen) return;
    const handleEsc = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onCancel();
    };
    window.addEventListener('keydown', handleEsc);
    return () => window.removeEventListener('keydown', handleEsc);
  }, [isOpen, onCancel]);

  if (!isOpen) return null;

  return (
    <div className="modal-overlay confirmation-modal-overlay" onClick={onCancel}>
      <div className="confirmation-modal-content" onClick={(e) => e.stopPropagation()}>
        <h3>{title}</h3>
        <p className="confirmation-modal-message">{message}</p>
        <div className="confirmation-modal-actions">
          <button onClick={onCancel} className="modal-button secondary">
            キャンセル
          </button>
          <button onClick={onConfirm} className="modal-button danger">
            はい、削除します
          </button>
        </div>
      </div>
    </div>
  );
};

const CropFormFields: FC<{
  formData: Partial<FormData>;
  onFormChange: (e: ChangeEvent<HTMLInputElement | HTMLSelectElement>) => void;
  cropName: string;
  isEditMode: boolean;
  varietyOptions: VarietyOption[];
  varietiesLoading: boolean;
  varietiesError: string | null;
  onSelectVariety: (option: VarietyOption | null) => void;
  varietySearchEnabled?: boolean;
  varietySearchQuery?: string;
  onVarietySearchChange?: (value: string) => void;
  hasVarietyLookupData?: boolean;
  tillageOptions: TillageOption[];
  tillageLoading: boolean;
  tillageError: string | null;
  onSelectTillage: (option: TillageOption | null) => void;
  seedingTillageSystemOptions: TillageSystemOption[];
  seedingTillageSystemLoading: boolean;
  seedingTillageSystemError: string | null;
  onSelectSeedingTillageSystem: (option: TillageSystemOption | null) => void;
}> = ({
  formData,
  onFormChange,
  cropName,
  isEditMode,
  varietyOptions,
  varietiesLoading,
  varietiesError,
  onSelectVariety,
  varietySearchEnabled = false,
  varietySearchQuery,
  onVarietySearchChange,
  hasVarietyLookupData = false,
  tillageOptions,
  tillageLoading,
  tillageError,
  onSelectTillage,
  seedingTillageSystemOptions,
  seedingTillageSystemLoading,
  seedingTillageSystemError,
  onSelectSeedingTillageSystem,
}) => {
  const showPlantingMethod = CROP_DATA.NEEDS_PLANTING_METHOD.includes(cropName);
  const showTillage = needsTillage(cropName);
  const requiresSeedingTillageSystem = needsSeedingTillageSystem(cropName);
  const [isVarietyDropdownOpen, setIsVarietyDropdownOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement | null>(null);

  let growthStageOptions: string[] = [];
  if (cropName) {
    const method = formData.planting_method;
    if ((cropName === '稲' || cropName === 'てんさい') && method) {
      growthStageOptions = (CROP_DATA.GROWTH_STAGE_OPTIONS[cropName as keyof typeof CROP_DATA.GROWTH_STAGE_OPTIONS] as any)[method] || [];
    } else {
      growthStageOptions = CROP_DATA.GROWTH_STAGE_OPTIONS.default as string[];
    }
  }

  const idPrefix = isEditMode ? 'edit' : 'template';
  const showVarietySearch = Boolean((varietySearchEnabled || isEditMode) && hasVarietyLookupData && onVarietySearchChange);
  const varietySearchValue = varietySearchQuery ?? (formData.variety || '');
    const effectiveTillageOptions = useMemo(() => {
    if (!formData.tillageUuid || !formData.tillage) return tillageOptions;
    if (tillageOptions.some((opt) => opt.uuid === formData.tillageUuid)) {
      return tillageOptions;
    }
    return [
      {
        uuid: formData.tillageUuid,
        name: formData.tillage,
        code: undefined,
        description: undefined,
      },
      ...tillageOptions,
    ];
  }, [tillageOptions, formData.tillageUuid, formData.tillage]);

const effectiveSeedingOptions = useMemo(() => {
    if (!formData.seedingTillageSystemUuid || !formData.seedingTillageSystemName) return seedingTillageSystemOptions;
    if (seedingTillageSystemOptions.some((opt) => opt.uuid === formData.seedingTillageSystemUuid)) {
      return seedingTillageSystemOptions;
    }
    return [
      {
        uuid: formData.seedingTillageSystemUuid,
        name: formData.seedingTillageSystemName,
        code: undefined,
        description: undefined,
      },
      ...seedingTillageSystemOptions,
    ];
  }, [seedingTillageSystemOptions, formData.seedingTillageSystemUuid, formData.seedingTillageSystemName]);

  useEffect(() => {
    if (!isVarietyDropdownOpen) return;
    const handleClickOutside = (event: MouseEvent | TouchEvent) => {
      if (!dropdownRef.current) return;
      if (!dropdownRef.current.contains(event.target as Node)) {
        setIsVarietyDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    document.addEventListener('touchstart', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
      document.removeEventListener('touchstart', handleClickOutside);
    };
  }, [isVarietyDropdownOpen]);

  useEffect(() => {
    if (!hasVarietyLookupData) {
      setIsVarietyDropdownOpen(false);
    }
  }, [hasVarietyLookupData]);

  const toggleVarietyDropdown = () => {
    if (!hasVarietyLookupData || varietiesLoading || varietiesError) return;
    onVarietySearchChange?.(formData.variety || '');
    setIsVarietyDropdownOpen((prev) => !prev);
  };

  const handleVarietySearchChange = (value: string) => {
    onVarietySearchChange?.(value);
    onSelectVariety(null);
  };

  const handleVarietySelect = (option: VarietyOption) => {
    onSelectVariety(option);
    onVarietySearchChange?.(option.name);
    setIsVarietyDropdownOpen(false);
  };

  return (
    <div className="form-sections-container">
      <div className="form-section-card">
        <h3 className="form-section-title">基本情報</h3>
        <div className="form-grid">
          <div className="form-group">
            <label>作物名</label>
            <input type="text" value={cropName} disabled />
          </div>
          <div className="form-group">
            <label htmlFor={`${idPrefix}_variety`}>品種名</label>
            {varietiesLoading ? (
              <button type="button" className="variety-dropdown-toggle" disabled>
                読み込み中...
              </button>
            ) : varietiesError ? (
              <span className="form-helper-text" style={{ color: '#ff6b6b' }}>{varietiesError}</span>
            ) : varietyOptions.length > 0 ? (
              <>
                <div className={`variety-dropdown ${isVarietyDropdownOpen ? 'open' : ''}`} ref={dropdownRef}>
                  <button
                    type="button"
                    className="variety-dropdown-toggle"
                    onClick={toggleVarietyDropdown}
                    aria-haspopup="listbox"
                    aria-expanded={isVarietyDropdownOpen}
                  >
                    <span>{formData.variety || '選択してください'}</span>
                  </button>
                  {isVarietyDropdownOpen && (
                    <div className="variety-dropdown-menu">
                      {showVarietySearch && (
                        <input
                          type="search"
                          value={varietySearchValue}
                          onChange={(e) => handleVarietySearchChange(e.target.value)}
                          placeholder="品種を検索..."
                          className="variety-dropdown-search"
                          autoFocus
                        />
                      )}
                      <div className="variety-options-list" role="listbox">
                        {varietyOptions.map((opt) => {
                          const isSelected = formData.varietyUuid === opt.uuid;
                          return (
                            <button
                              type="button"
                              key={opt.uuid || opt.name}
                              className={`variety-option ${isSelected ? 'selected' : ''}`}
                              onClick={() => handleVarietySelect(opt)}
                              role="option"
                              aria-selected={isSelected}
                            >
                              <span className="variety-option-name">{opt.name}</span>
                              {opt.registrationNumber && <span className="variety-option-meta">登録番号: {opt.registrationNumber}</span>}
                            </button>
                          );
                        })}
                      </div>
                    </div>
                  )}
                </div>
                {isEditMode && formData.varietyUuid && (
                  <span className="form-helper-text" style={{ color: '#a0a0ab' }}>UUID: {formData.varietyUuid}</span>
                )}
              </>
            ) : (
              <>
                <input
                  type="text"
                  name="variety"
                  id={`${idPrefix}_variety`}
                  value={formData.variety || ''}
                  onChange={onFormChange}
                  placeholder="例: コシヒカリ"
                />
                {isEditMode && (
                  <span className="form-helper-text" style={{ color: formData.varietyUuid ? '#a0a0ab' : '#ff6b6b' }}>
                    {formData.varietyUuid ? `UUID: ${formData.varietyUuid}` : 'UUIDが設定されていません。保存前に正しい品種を再選択してください。'}
                  </span>
                )}
              </>
            )}
            {!isEditMode && !varietiesLoading && !varietiesError && !hasVarietyLookupData && (
              <span className="form-helper-text" style={{ color: '#ff6b6b' }}>
                該当する品種がありません。条件を変更するか、後ほど再度お試しください。
              </span>
            )}
          </div>
          {isEditMode && (
            <div className="form-group">
              <label htmlFor="edit_planting_date">作付日</label>
              <input type="date" name="planting_date" id="edit_planting_date" value={formData.planting_date} onChange={onFormChange} />
            </div>
          )}
        </div>
      </div>

      <div className="form-section-card">
        <h3 className="form-section-title">栽培情報</h3>
        <div className="form-grid">
          {showPlantingMethod && (
            <div className="form-group">
              <label htmlFor={`${idPrefix}_planting_method`}>作付け方法</label>
              <select name="planting_method" id={`${idPrefix}_planting_method`} value={formData.planting_method || ''} onChange={onFormChange}>
                {CROP_DATA.PLANTING_METHOD_OPTIONS[cropName as keyof typeof CROP_DATA.PLANTING_METHOD_OPTIONS]?.map((opt) => (
                  <option key={opt} value={opt}>
                    {opt}
                  </option>
                ))}
              </select>
            </div>
          )}
          {requiresSeedingTillageSystem && (seedingTillageSystemLoading ? (
            <div className="form-group">
              <label>播種方式 (Tillage System)</label>
              <select value="" disabled>
                <option value="">読み込み中...</option>
              </select>
            </div>
          ) : seedingTillageSystemError ? (
            <div className="form-group">
              <label>播種方式 (Tillage System)</label>
              <span className="form-helper-text" style={{ color: '#ff6b6b' }}>{seedingTillageSystemError}</span>
            </div>
          ) : effectiveSeedingOptions.length > 0 ? (
            <div className="form-group">
              <label htmlFor={`${idPrefix}_seeding_tillage_system`}>播種方式 (Tillage System)</label>
              <select
                id={`${idPrefix}_seeding_tillage_system`}
                value={formData.seedingTillageSystemUuid || ''}
                onChange={(e) => {
                  const option = effectiveSeedingOptions.find((item) => item.uuid === e.target.value) || null;
                  onSelectSeedingTillageSystem(option);
                }}
              >
                <option value="">選択してください</option>
                {effectiveSeedingOptions.map((opt) => (
                  <option key={opt.uuid || opt.name} value={opt.uuid}>
                    {opt.name}
                    {opt.code ? `（コード: ${opt.code}）` : ''}
                  </option>
                ))}
              </select>
            </div>
          ) : null)}
          <div className="form-group">
            <label htmlFor={`${idPrefix}_growth_stage`}>生育ステージ</label>
            <select name="growth_stage" id={`${idPrefix}_growth_stage`} value={formData.growth_stage || ''} onChange={onFormChange}>
              {growthStageOptions.map((opt) => (
                <option key={opt} value={opt}>
                  {opt}
                </option>
              ))}
            </select>
          </div>
          {showTillage && (
            <div className="form-group">
              <label htmlFor={`${idPrefix}_tillage`}>耕起</label>
              {tillageLoading ? (
                <select name="tillage" id={`${idPrefix}_tillage`} value="" disabled>
                  <option value="">読み込み中...</option>
                </select>
              ) : tillageError ? (
                <span className="form-helper-text" style={{ color: '#ff6b6b' }}>{tillageError}</span>
              ) : effectiveTillageOptions.length > 0 ? (
                <select
                  name="tillage"
                  id={`${idPrefix}_tillage`}
                  value={formData.tillageUuid || ''}
                  onChange={(e) => {
                    const option = effectiveTillageOptions.find((item) => item.uuid === e.target.value) || null;
                    onSelectTillage(option);
                  }}
                >
                  <option value="">選択してください</option>
                  {effectiveTillageOptions.map((opt) => (
                    <option key={opt.uuid || opt.name} value={opt.uuid}>
                      {opt.name}
                      {opt.code ? `（コード: ${opt.code}）` : ''}
                    </option>
                  ))}
                </select>
              ) : (
                <span className="form-helper-text" style={{ color: '#ff6b6b' }}>
                  利用できる耕起情報がありません。
                </span>
              )}
            </div>
          )}
        </div>
      </div>

      <div className="form-section-card">
        <h3 className="form-section-title">収量・履歴</h3>
        <div className="form-grid">
          <div className="form-group">
            <label htmlFor={`${idPrefix}_yield`}>予想収量 (kg/10a)</label>
            <input
              type="number"
              name="yield"
              id={`${idPrefix}_yield`}
              value={formData.yield || ''}
              onChange={onFormChange}
              placeholder="例: 500"
            />
          </div>
          <div className="form-group">
            <label htmlFor={`${idPrefix}_previous_crop`}>前作</label>
            <select name="previous_crop" id={`${idPrefix}_previous_crop`} value={formData.previous_crop || ''} onChange={onFormChange}>
              {CROP_DATA.PREVIOUS_CROP_OPTIONS.map((opt) => (
                <option key={opt} value={opt}>
                  {opt}
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>
    </div>
  );
};

const Calendar: FC<{
  currentDate: Date;
  setCurrentDate: (date: Date) => void;
  registrations?: Record<string, number>;
  onDateClick: (date: string) => void;
  selectionMode?: boolean;
  selectedDate?: string | null;
}> = ({ currentDate, setCurrentDate, registrations = {}, onDateClick, selectionMode = false, selectedDate = null }) => {
  const year = currentDate.getFullYear();
  const month = currentDate.getMonth();
  const startDay = new Date(year, month, 1).getDay();
  const daysInMonth = new Date(year, month + 1, 0).getDate();
  const todayString = new Date().toISOString().split('T')[0];

  const calendarDays = useMemo(() => {
    const days: ReactNode[] = [];

    for (let i = 0; i < startDay; i += 1) {
      days.push(<div key={`empty-start-${i}`} className="calendar-day empty" />);
    }

    for (let i = 1; i <= daysInMonth; i += 1) {
      const dateObj = new Date(Date.UTC(year, month, i));
      const dateString = dateObj.toISOString().split('T')[0];
      const dayOfWeek = dateObj.getUTCDay();
      const registrationCount = registrations[dateString] || 0;

      const dayClasses = ['calendar-day'];
      if (dateString === todayString) dayClasses.push('today');
      if (dateString === selectedDate && !selectionMode) dayClasses.push('selected');
      if (dayOfWeek === 0) dayClasses.push('sunday');
      if (dayOfWeek === 6) dayClasses.push('saturday');
      if (selectionMode) dayClasses.push('selection-mode');

      days.push(
        <div key={dateString} className={dayClasses.join(' ')} onClick={() => onDateClick(dateString)} role="button">
          <span>{i}</span>
          {!selectionMode && registrationCount > 0 && (
            <div className="registration-count" title={`${registrationCount}件の登録`}>
              {registrationCount}
            </div>
          )}
        </div>,
      );
    }

    const totalCells = startDay + daysInMonth;
    const remainingCells = (7 - (totalCells % 7)) % 7;
    for (let i = 0; i < remainingCells; i += 1) {
      days.push(<div key={`empty-end-${i}`} className="calendar-day empty" />);
    }
    return days;
  }, [year, month, startDay, daysInMonth, registrations, onDateClick, selectionMode, selectedDate, todayString]);

  const prevMonth = () => setCurrentDate(new Date(year, month - 1, 1));
  const nextMonth = () => setCurrentDate(new Date(year, month + 1, 1));

  return (
    <div className="calendar-container">
      <div className="calendar-controls">
        <button onClick={prevMonth} aria-label="前の月へ">
          &lt;
        </button>
        <h3>
          {year}年 {month + 1}月
        </h3>
        <button onClick={nextMonth} aria-label="次の月へ">
          &gt;
        </button>
      </div>
      <div className="calendar-body">
        <div className="calendar-grid">
          <div className="day-name sunday">日</div>
          <div className="day-name">月</div>
          <div className="day-name">火</div>
          <div className="day-name">水</div>
          <div className="day-name">木</div>
          <div className="day-name">金</div>
          <div className="day-name saturday">土</div>
          {calendarDays}
        </div>
      </div>
    </div>
  );
};

const EditRegistrationForm: FC<{
  registrationData: FormData;
  onSave: (data: FormData) => void;
  onCancel: () => void;
  onDelete: (fieldId: string) => void;
  tillageSystems: TillageOption[];
  tillageSystemsLoading: boolean;
  tillageSystemsError: string | null;
  onEnsureTillageSystems: () => Promise<void>;
}> = ({ registrationData, onSave, onCancel, onDelete, tillageSystems, tillageSystemsLoading, tillageSystemsError, onEnsureTillageSystems }) => {
  const { auth } = useAuth();
  const [formData, setFormData] = useState<FormData>(registrationData);
  const [isConfirmModalOpen, setIsConfirmModalOpen] = useState(false);
  const [varietyOptions, setVarietyOptions] = useState<VarietyOption[]>([]);
  const [varietiesLoading, setVarietiesLoading] = useState(false);
  const [varietiesError, setVarietiesError] = useState<string | null>(null);
  const [varietySearchQuery, setVarietySearchQuery] = useState(registrationData.variety || '');
  const [cropOptions, setCropOptions] = useState<CropOption[]>([]);
  const { crop_name, planting_method, growth_stage } = formData;
  const showTillage = needsTillage(formData.crop_name);
  const cropNameToUuid = useMemo(() => {
    const map = new Map<string, string>();
    cropOptions.forEach((option) => {
      if (option.uuid) {
        map.set(option.name, option.uuid);
      }
    });
    return map;
  }, [cropOptions]);

  useEffect(() => {
    if (!auth) return;
    let cancelled = false;

    const fetchCrops = async () => {
      try {
        const { ok, status, json } = await postJsonCached<any>(
          withApiBase('/masterdata/crops'),
          {
            login_token: auth.login.login_token,
            api_token: auth.api_token,
            locale: 'JA-JP',
          },
          undefined,
          { cacheKey: 'masterdata:crops:JA-JP', cache: 'session' },
        );
        if (!ok) throw new Error(`HTTP ${status}`);
        const items = (json.items ?? json ?? []) as any[];
        const normalized = items
          .map((item) => ({
            uuid: item?.uuid ?? '',
            name: item?.name ?? item?.scientificName ?? item?.code ?? '',
            eppoCode: item?.code ?? item?.eppoCode ?? undefined,
            scientificName: item?.scientificName ?? undefined,
          }))
          .filter((item) => item.uuid && item.name)
          .filter((item) => ALLOWED_CROP_NAME_SET.has(item.name))
          .sort((a, b) => a.name.localeCompare(b.name, 'ja'));
        if (!cancelled) {
          setCropOptions(normalized);
        }
      } catch (error) {
        if (!cancelled) {
          console.warn('[CropRegistration] failed to load crops for edit form', error);
          setCropOptions([]);
        }
      }
    };

    fetchCrops();

    return () => {
      cancelled = true;
    };
  }, [auth]);

  useEffect(() => {
    if (showTillage) {
      void onEnsureTillageSystems();
    } else {
      setFormData((prev) => ({ ...prev, tillage: '', tillageUuid: '' }));
    }
  }, [showTillage, onEnsureTillageSystems]);

  const handleSelectTillage = (option: TillageOption | null) => {
    setFormData((prev) => ({
      ...prev,
      tillage: option?.name ?? '',
      tillageUuid: option?.uuid ?? '',
    }));
  };


  const handleFormChange = (e: ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    const { name, value } = e.target;
    if (name === 'variety') {
      setVarietySearchQuery(value);
      setFormData((prev) => ({ ...prev, variety: value, varietyUuid: '' }));
      return;
    }
     if (name === 'previous_crop') {
       setFormData((prev) => ({
         ...prev,
         previous_crop: value,
         previousCropUuid: value && value !== '該当なし' ? cropNameToUuid.get(value) ?? '' : '',
       }));
       return;
     }
    setFormData((prev) => ({ ...prev, [name]: value }));
  };


  const handleSelectSeedingTillageSystem = (option: TillageSystemOption | null) => {
    setFormData((prev) => ({
      ...prev,
      seedingTillageSystemUuid: option?.uuid ?? '',
      seedingTillageSystemName: option?.name ?? '',
    }));
  };

  useEffect(() => {
    setVarietySearchQuery(formData.variety || '');
  }, [formData.variety]);

  useEffect(() => {
    if (!auth || !registrationData.cropUuid) {
      setVarietyOptions([]);
      setVarietiesError(null);
      setVarietiesLoading(false);
      return;
    }

    let cancelled = false;

    const fetchVarieties = async () => {
      setVarietiesLoading(true);
      setVarietiesError(null);
      try {
        const { ok, status, json } = await postJsonCached<any>(
          withApiBase('/masterdata/varieties'),
          {
            login_token: auth.login.login_token,
            api_token: auth.api_token,
            locale: 'JA-JP',
            countryCode: 'JP',
            cropUuid: registrationData.cropUuid,
          },
          undefined,
          { cacheKey: `masterdata:varieties:JA-JP:JP:${registrationData.cropUuid}`, cache: 'session' },
        );
        if (!ok) {
          const detail = typeof json === 'string' ? json.slice(0, 200) : '';
          throw new Error(`HTTP ${status}${detail ? `: ${detail}` : ''}`);
        }
        const items = (json.items ?? json ?? []) as any[];
        const normalized = items
          .map((item) => ({
            uuid: item?.uuid ?? '',
            name: item?.name ?? item?.code ?? '',
            code: item?.code ?? undefined,
            registrationNumber: item?.registrationNumber ?? undefined,
          }))
          .filter((item) => item.uuid && item.name)
          .sort((a, b) => a.name.localeCompare(b.name, 'ja'));
        if (!cancelled) {
          setVarietyOptions(normalized);
        }
      } catch (error) {
        if (!cancelled) {
          const message = error instanceof Error ? error.message : '品種の取得に失敗しました';
          setVarietiesError(message);
          setVarietyOptions([]);
        }
      } finally {
        if (!cancelled) {
          setVarietiesLoading(false);
        }
      }
    };

    fetchVarieties();

    return () => {
      cancelled = true;
    };
  }, [auth, registrationData.cropUuid, registrationData.varietyUuid]);

  useEffect(() => {
    if (CROP_DATA.NEEDS_PLANTING_METHOD.includes(crop_name)) {
      const base = CROP_DATA.GROWTH_STAGE_OPTIONS[crop_name as keyof typeof CROP_DATA.GROWTH_STAGE_OPTIONS] as
        | Record<string, string[]>
        | undefined;
      let growthStageOptions: string[] = [];
      if (base && planting_method) {
        growthStageOptions = base[planting_method] ?? [];
      }
      if (growthStageOptions.length > 0 && !growthStageOptions.includes(growth_stage)) {
        setFormData((prev) => ({ ...prev, growth_stage: growthStageOptions[0] || '' }));
      }
    }
  }, [crop_name, planting_method, growth_stage]);

  const handleSave = () => {
    const {
      cropUuid,
      variety,
      varietyUuid,
      yield: targetYield,
      growth_stage,
      previous_crop,
      planting_method,
      tillage,
      tillageUuid,
      planting_date,
      crop_name,
      seedingTillageSystemUuid,
    } = formData;
    if (!cropUuid || !variety || !varietyUuid || !targetYield || !growth_stage || !previous_crop || !planting_date) {
      window.alert('必須項目をすべて入力してください。');
      return;
    }
    if (CROP_DATA.NEEDS_PLANTING_METHOD.includes(crop_name) && !planting_method) {
      window.alert('作付け方法を選択してください。');
      return;
    }
    if (showTillage && (!tillage || !tillageUuid)) {
      window.alert('耕起を選択してください。');
      return;
    }
    if (needsSeedingTillageSystem(crop_name) && !seedingTillageSystemUuid) {
      window.alert('播種方式を選択してください。');
      return;
    }
    if (previous_crop && previous_crop !== '該当なし' && !formData.previousCropUuid) {
      window.alert('前作の情報を再選択してください。');
      return;
    }
    onSave(formData);
  };

  return (
    <>
      <div className="edit-form-container wizard-step">
        <div className="details-header">
          <h2>作付情報の編集</h2>
        </div>
        <div className="wizard-step-body">
          <div className="form-section-card">
            <h3 className="form-section-title">圃場情報</h3>
            <div className="form-grid">
              <div className="form-group">
                <label>圃場名</label>
                <input type="text" value={formData.fieldName} disabled />
              </div>
              <div className="form-group">
                <label>作物名</label>
                <input type="text" value={formData.crop_name} disabled />
              </div>
            </div>
          </div>
          <CropFormFields
            formData={formData}
            onFormChange={handleFormChange}
            cropName={formData.crop_name}
            isEditMode
            varietyOptions={varietyOptions}
            varietiesLoading={varietiesLoading}
            varietiesError={varietiesError}
            onSelectVariety={(option) => {
              setVarietySearchQuery(option?.name ?? '');
              setFormData((prev) => ({
                ...prev,
                variety: option?.name ?? prev.variety,
                varietyUuid: option?.uuid ?? prev.varietyUuid,
              }));
            }}
            varietySearchEnabled
            varietySearchQuery={varietySearchQuery}
            onVarietySearchChange={setVarietySearchQuery}
            hasVarietyLookupData={varietyOptions.length > 0}
            tillageOptions={tillageSystems}
            tillageLoading={tillageSystemsLoading}
            tillageError={tillageSystemsError}
            onSelectTillage={handleSelectTillage}
            seedingTillageSystemOptions={tillageSystems}
            seedingTillageSystemLoading={tillageSystemsLoading}
            seedingTillageSystemError={tillageSystemsError}
            onSelectSeedingTillageSystem={handleSelectSeedingTillageSystem}
          />
        </div>
        <div className="wizard-nav">
          <button className="wizard-nav-button delete-button" onClick={() => setIsConfirmModalOpen(true)}>
            削除する
          </button>
          <div className="wizard-nav-actions">
            <button className="wizard-nav-button secondary" onClick={onCancel}>
              キャンセル
            </button>
            <button className="wizard-nav-button" onClick={handleSave}>
              保存する
            </button>
          </div>
        </div>
      </div>
      <ConfirmationModal
        isOpen={isConfirmModalOpen}
        onConfirm={() => {
          onDelete(formData.fieldId);
          setIsConfirmModalOpen(false);
        }}
        onCancel={() => setIsConfirmModalOpen(false)}
        title="登録の削除の確認"
        message="この作付け登録を本当に削除しますか？この操作は元に戻せません。"
      />
    </>
  );
};

const CalendarViewScreen: FC<{
  registrationHistory: FormData[];
  onClose: () => void;
  onUpdateRegistration: (data: FormData) => void;
  onUnregister: (fieldId: string) => void;
  tillageSystems: TillageOption[];
  tillageSystemsLoading: boolean;
  tillageSystemsError: string | null;
  onEnsureTillageSystems: () => Promise<void>;
}> = ({ registrationHistory, onClose, onUpdateRegistration, onUnregister, tillageSystems, tillageSystemsLoading, tillageSystemsError, onEnsureTillageSystems }) => {
  const [currentDate, setCurrentDate] = useState(new Date());
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const [editingRegistration, setEditingRegistration] = useState<FormData | null>(null);
  const [tableSearch, setTableSearch] = useState('');
  const [prefectureFilter, setPrefectureFilter] = useState('ALL');
  const [municipalityFilter, setMunicipalityFilter] = useState('ALL');

  const registrationsByDate = useMemo(
    () =>
      registrationHistory.reduce((acc, reg) => {
        acc[reg.planting_date] = (acc[reg.planting_date] || 0) + 1;
        return acc;
      }, {} as Record<string, number>),
    [registrationHistory],
  );

  const registrationsOnSelectedDate = useMemo(
    () =>
      selectedDate
        ? registrationHistory
            .filter((reg) => reg.planting_date === selectedDate)
            .sort((a, b) => a.fieldName.localeCompare(b.fieldName))
        : [],
    [registrationHistory, selectedDate],
  );

  const prefectureOptions = useMemo(() => {
    const set = new Set<string>();
    registrationHistory.forEach((reg) => {
      if (reg.prefecture) set.add(reg.prefecture);
    });
    return ['ALL', ...Array.from(set).sort((a, b) => a.localeCompare(b, 'ja'))];
  }, [registrationHistory]);

  const municipalityOptions = useMemo(() => {
    const set = new Set<string>();
    registrationHistory.forEach((reg) => {
      if (prefectureFilter !== 'ALL' && reg.prefecture !== prefectureFilter) return;
      if (reg.municipality) set.add(reg.municipality);
    });
    return ['ALL', ...Array.from(set).sort((a, b) => a.localeCompare(b, 'ja'))];
  }, [registrationHistory, prefectureFilter]);

  const filteredTableRegistrations = useMemo(() => {
    const query = tableSearch.trim().toLowerCase();
    return [...registrationHistory]
      .filter((reg) => {
        if (prefectureFilter !== 'ALL' && reg.prefecture !== prefectureFilter) return false;
        if (municipalityFilter !== 'ALL' && reg.municipality !== municipalityFilter) return false;
        if (query) {
          const haystack = [
            reg.planting_date,
            reg.fieldName,
            reg.crop_name,
            reg.variety,
            reg.prefecture,
            reg.municipality,
          ]
            .join(' ')
            .toLowerCase();
          if (!haystack.includes(query)) return false;
        }
        return true;
      })
      .sort((a, b) => b.planting_date.localeCompare(a.planting_date));
  }, [registrationHistory, tableSearch, prefectureFilter, municipalityFilter]);

  const {
    paginatedData: paginatedTableRows,
    PaginationControls: TablePagination,
    setCurrentPage: setTablePage,
  } = usePagination(filteredTableRegistrations, 10);

  useEffect(() => {
    setTablePage(1);
  }, [tableSearch, prefectureFilter, municipalityFilter, setTablePage]);

  useEffect(() => {
    if (editingRegistration && needsTillage(editingRegistration.crop_name)) {
      void onEnsureTillageSystems();
    }
  }, [editingRegistration, onEnsureTillageSystems]);

  const handleSaveEdit = (updatedData: FormData) => {
    onUpdateRegistration(updatedData);
    setEditingRegistration(null);
    if (updatedData.planting_date !== selectedDate) {
      setSelectedDate(updatedData.planting_date);
    }
  };

  const handleTableEdit = (registration: FormData) => {
    if (needsTillage(registration.crop_name)) {
      void onEnsureTillageSystems();
    }
    setSelectedDate(registration.planting_date);
    setEditingRegistration(registration);
  };

  const handleTableDelete = (registration: FormData) => {
    if (window.confirm(`「${registration.fieldName}」の作付登録を削除しますか？`)) {
      onUnregister(registration.fieldId);
    }
  };

  if (editingRegistration) {
    return (
      <EditRegistrationForm
        registrationData={editingRegistration}
        onSave={handleSaveEdit}
        onCancel={() => setEditingRegistration(null)}
        onDelete={(fieldId) => {
          onUnregister(fieldId);
          setEditingRegistration(null);
        }}
        tillageSystems={tillageSystems}
        tillageSystemsLoading={tillageSystemsLoading}
        tillageSystemsError={tillageSystemsError}
        onEnsureTillageSystems={onEnsureTillageSystems}
      />
    );
  }

  return (
    <div className="details-screen">
      <div className="details-header">
        <h2>登録カレンダー</h2>
        <button onClick={onClose} className="cancel-button">
          閉じる
        </button>
      </div>
      <div className="calendar-view-body">
        <div className="calendar-view-main">
          <Calendar
            currentDate={currentDate}
            setCurrentDate={setCurrentDate}
            registrations={registrationsByDate}
            onDateClick={(date) => {
              setSelectedDate(date);
              setEditingRegistration(null);
            }}
            selectedDate={selectedDate}
          />
        </div>
        <aside className="calendar-view-details">
          {selectedDate ? (
            <div className="daily-registrations-container">
              <h3>
                {new Date(`${selectedDate}T00:00:00Z`).toLocaleDateString('ja-JP', {
                  timeZone: 'UTC',
                  year: 'numeric',
                  month: 'long',
                  day: 'numeric',
                })}
                の登録
              </h3>
              {registrationsOnSelectedDate.length > 0 ? (
                <div className="daily-registrations-list">
                  {registrationsOnSelectedDate.map((reg) => (
                    <div key={reg.fieldId} className="daily-registration-card">
                      <div className="field-info">
                        <span className="field-name">{reg.fieldName}</span>
                        <div className="field-details">
                          <span>
                            {CROP_DATA.ICONS[reg.crop_name] || '🌱'} {reg.crop_name} ({reg.variety})
                          </span>
                        </div>
                      </div>
                      <button
                        onClick={() => setEditingRegistration(reg)}
                        className="edit-button"
                        aria-label={`${reg.fieldName}の登録を編集`}
                      >
                        編集
                      </button>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="no-registrations-message">
                  <p>この日の登録はありません。</p>
                </div>
              )}
            </div>
          ) : (
            <div className="no-date-selected-message">
              <p>カレンダーの日付をクリックして、登録内容を確認・編集できます。</p>
            </div>
          )}
        </aside>
      </div>
      <div className="calendar-table-section">
        <div className="calendar-table-filters">
          <div className="filter-group">
            <label htmlFor="calendar-table-search">検索</label>
            <input
              id="calendar-table-search"
              type="search"
              placeholder="圃場・作物・品種などを検索..."
              value={tableSearch}
              onChange={(e) => setTableSearch(e.target.value)}
            />
          </div>
          <div className="filter-group">
            <label htmlFor="calendar-filter-prefecture">都道府県</label>
            <select
              id="calendar-filter-prefecture"
              value={prefectureFilter}
              onChange={(e) => {
                setPrefectureFilter(e.target.value);
                setMunicipalityFilter('ALL');
              }}
            >
              {prefectureOptions.map((opt) => (
                <option key={opt} value={opt}>
                  {opt === 'ALL' ? 'すべて' : opt}
                </option>
              ))}
            </select>
          </div>
          <div className="filter-group">
            <label htmlFor="calendar-filter-municipality">市区町村</label>
            <select
              id="calendar-filter-municipality"
              value={municipalityFilter}
              onChange={(e) => setMunicipalityFilter(e.target.value)}
            >
              {municipalityOptions.map((opt) => (
                <option key={opt} value={opt}>
                  {opt === 'ALL' ? 'すべて' : opt}
                </option>
              ))}
            </select>
          </div>
        </div>
        <div className="calendar-table-wrapper">
          <table className="calendar-table">
            <thead>
              <tr>
                <th>作付日</th>
                <th>圃場名</th>
                <th>作物名</th>
                <th>品種名</th>
                <th>都道府県</th>
                <th>市区町村</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {paginatedTableRows.length > 0 ? (
                paginatedTableRows.map((reg) => (
                  <tr key={reg.fieldId}>
                    <td>{new Date(`${reg.planting_date}T00:00:00Z`).toLocaleDateString('ja-JP', { timeZone: 'UTC' })}</td>
                    <td>{reg.fieldName}</td>
                    <td>{reg.crop_name}</td>
                    <td>{reg.variety || '-'}</td>
                    <td>{reg.prefecture || '-'}</td>
                    <td>{reg.municipality || '-'}</td>
                    <td>
                      <div className="calendar-table-actions">
                        <button type="button" className="btn-edit" onClick={() => handleTableEdit(reg)}>
                          編集
                        </button>
                        <button type="button" className="btn-delete" onClick={() => handleTableDelete(reg)}>
                          削除
                        </button>
                      </div>
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={7} className="calendar-table-empty">
                    条件に一致する登録がありません。
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        <TablePagination className="calendar-table-pagination" />
      </div>
    </div>
  );
};

type UserField = { id: string; name: string; prefecture: string; municipality: string };

const RegistrationDetailsScreen: FC<{
  onRegister: (selections: FormData[]) => void;
  onCancel: () => void;
  registrationHistory: FormData[];
  onUnregister: (fieldId: string) => void;
  userFields: UserField[];
  tillageSystems: TillageOption[];
  tillageSystemsLoading: boolean;
  tillageSystemsError: string | null;
  onEnsureTillageSystems: () => Promise<void>;
}> = ({
  onRegister,
  onCancel,
  registrationHistory,
  onUnregister,
  userFields,
  tillageSystems,
  tillageSystemsLoading,
  tillageSystemsError,
  onEnsureTillageSystems,
}) => {
  const { auth } = useAuth();
  const [step, setStep] = useState(1);
  const [cropName, setCropName] = useState<string>('');
  const [selectedCrop, setSelectedCrop] = useState<CropOption | null>(null);
  const [cropSearchQuery, setCropSearchQuery] = useState('');
  const [templateValues, setTemplateValues] = useState<Partial<Omit<FormData, 'fieldId' | 'fieldName' | 'crop_name'>>>({});
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedFieldIds, setSelectedFieldIds] = useState<Set<string>>(new Set());
  const [calendarDate, setCalendarDate] = useState(new Date());
  const [isRegisteredListCollapsed, setIsRegisteredListCollapsed] = useState(true);
  const [cropOptions, setCropOptions] = useState<CropOption[]>([]);
  const [cropsLoading, setCropsLoading] = useState(false);
  const [cropFetchError, setCropFetchError] = useState<string | null>(null);
  const [varietyOptions, setVarietyOptions] = useState<VarietyOption[]>([]);
  const [varietiesLoading, setVarietiesLoading] = useState(false);
  const [varietiesError, setVarietiesError] = useState<string | null>(null);
  const [varietySearchQuery, setVarietySearchQuery] = useState('');
  const [pendingRegistrations, setPendingRegistrations] = useState<FormData[] | null>(null);
  const [pendingPayloads, setPendingPayloads] = useState<CropSeasonCreatePayload[]>([]);
  const [isConfirmationOpen, setIsConfirmationOpen] = useState(false);
  const [submissionLoading, setSubmissionLoading] = useState(false);
  const [submissionError, setSubmissionError] = useState<string | null>(null);

  const registeredFieldsMap = useMemo(() => new Map(registrationHistory.map((reg) => [reg.fieldId, reg])), [registrationHistory]);

  useEffect(() => {
    if (step === 2 && needsTillage(cropName)) {
      void onEnsureTillageSystems();
    }
  }, [step, cropName, onEnsureTillageSystems]);

  const handleSelectTillage = (option: TillageOption | null) => {
    setTemplateValues((prev) => ({
      ...prev,
      tillage: option?.name ?? '',
      tillageUuid: option?.uuid ?? '',
    }));
  };

  const handleSelectSeedingTillageSystem = (option: TillageSystemOption | null) => {
    setTemplateValues((prev) => ({
      ...prev,
      seedingTillageSystemUuid: option?.uuid ?? '',
      seedingTillageSystemName: option?.name ?? '',
    }));
  };

  useEffect(() => {
    if (!auth) return;

    let cancelled = false;

    const fetchCrops = async () => {
      setCropsLoading(true);
      setCropFetchError(null);
      try {
        const { ok, status, json } = await postJsonCached<any>(
          withApiBase('/masterdata/crops'),
          {
            login_token: auth.login.login_token,
            api_token: auth.api_token,
            locale: 'JA-JP',
          },
          undefined,
          { cacheKey: 'masterdata:crops:JA-JP', cache: 'session' },
        );
        if (!ok) {
          const detail = typeof json === 'string' ? json.slice(0, 200) : '';
          throw new Error(`HTTP ${status}${detail ? `: ${detail}` : ''}`);
        }
        const items = (json.items ?? json ?? []) as any[];
        const normalized = items
          .map((item) => ({
            uuid: item?.uuid ?? '',
            name: item?.name ?? item?.scientificName ?? item?.code ?? '',
            eppoCode: item?.code ?? item?.eppoCode ?? undefined,
            scientificName: item?.scientificName ?? undefined,
          }))
          .filter((item) => item.uuid && item.name)
          .sort((a, b) => a.name.localeCompare(b.name, 'ja'));
        if (!cancelled) {
          const filtered = normalized.filter((item) => ALLOWED_CROP_NAME_SET.has(item.name));
          setCropOptions(filtered);
        }
      } catch (error) {
        if (!cancelled) {
          const message = error instanceof Error ? error.message : '作物の取得に失敗しました';
          setCropFetchError(message);
        }
      } finally {
        if (!cancelled) {
          setCropsLoading(false);
        }
      }
    };

    fetchCrops();

    return () => {
      cancelled = true;
    };
  }, [auth?.api_token, auth?.login?.login_token, tillageSystemsLoading]);

  useEffect(() => {
    if (!auth || !selectedCrop?.uuid) {
      setVarietySearchQuery('');
      setVarietyOptions([]);
      setVarietiesError(null);
      setVarietiesLoading(false);
      return;
    }

    let cancelled = false;

    const fetchVarieties = async () => {
      setVarietiesLoading(true);
      setVarietiesError(null);
      try {
        const { ok, status, json } = await postJsonCached<any>(
          withApiBase('/masterdata/varieties'),
          {
            login_token: auth.login.login_token,
            api_token: auth.api_token,
            locale: 'JA-JP',
            countryCode: 'JP',
            cropUuid: selectedCrop.uuid,
          },
          undefined,
          { cacheKey: `masterdata:varieties:JA-JP:JP:${selectedCrop.uuid}`, cache: 'session' },
        );
        if (!ok) {
          const detail = typeof json === 'string' ? json.slice(0, 200) : '';
          throw new Error(`HTTP ${status}${detail ? `: ${detail}` : ''}`);
        }
        const items = (json.items ?? json ?? []) as any[];
        const normalized = items
          .map((item) => ({
            uuid: item?.uuid ?? '',
            name: item?.name ?? item?.code ?? '',
            code: item?.code ?? undefined,
            registrationNumber: item?.registrationNumber ?? undefined,
          }))
          .filter((item) => item.uuid && item.name)
          .sort((a, b) => a.name.localeCompare(b.name, 'ja'));
        if (!cancelled) {
          setVarietySearchQuery('');
          setVarietyOptions(normalized);
          let nextSelectedName = '';
          setTemplateValues((prev) => {
            if (normalized.length === 0) {
              if (!prev.variety && !prev.varietyUuid) return prev;
              nextSelectedName = '';
              return { ...prev, variety: '', varietyUuid: '' };
            }
            const currentByUuid = prev.varietyUuid
              ? normalized.find((opt) => opt.uuid === prev.varietyUuid)
              : undefined;
            if (currentByUuid) {
              nextSelectedName = currentByUuid.name ?? '';
              return {
                ...prev,
                variety: currentByUuid.name,
                varietyUuid: currentByUuid.uuid,
              };
            }
            const currentByName = prev.variety
              ? normalized.find((opt) => opt.name === prev.variety)
              : undefined;
            const nextVariety = currentByName ?? normalized[0];
            nextSelectedName = nextVariety?.name ?? '';
            return {
              ...prev,
              variety: nextVariety?.name ?? '',
              varietyUuid: nextVariety?.uuid ?? '',
            };
          });
          setVarietySearchQuery(nextSelectedName);
        }
      } catch (error) {
        if (!cancelled) {
          const message = error instanceof Error ? error.message : '品種の取得に失敗しました';
          setVarietiesError(message);
          setVarietyOptions([]);
          setTemplateValues((prev) => ({
            ...prev,
            variety: '',
            varietyUuid: '',
          }));
          setVarietySearchQuery('');
        }
      } finally {
        if (!cancelled) {
          setVarietiesLoading(false);
        }
      }
    };

    fetchVarieties();

    return () => {
      cancelled = true;
    };
  }, [auth, selectedCrop]);

  useEffect(() => {
    if (!cropName) {
      setTemplateValues({});
      return;
    }
    const initialMethod = CROP_DATA.PLANTING_METHOD_OPTIONS[cropName as keyof typeof CROP_DATA.PLANTING_METHOD_OPTIONS]?.[0] || '';
    let initialGrowthStage = (CROP_DATA.GROWTH_STAGE_OPTIONS.default as string[])[0] || '';
    const base = CROP_DATA.GROWTH_STAGE_OPTIONS[cropName as keyof typeof CROP_DATA.GROWTH_STAGE_OPTIONS] as
      | Record<string, string[]>
      | undefined;
    if (base && initialMethod) {
      initialGrowthStage = base[initialMethod]?.[0] || initialGrowthStage;
    }
    setTemplateValues({
      cropUuid: selectedCrop?.uuid ?? '',
      variety: '',
      varietyUuid: '',
      planting_method: initialMethod,
      growth_stage: initialGrowthStage,
      yield: '',
      previous_crop: '該当なし',
      previousCropUuid: '',
      tillage: '',
      tillageUuid: '',
      seedingTillageSystemUuid: '',
      seedingTillageSystemName: '',
    });
  }, [cropName, selectedCrop?.uuid]);

  const filteredFields = useMemo(
    () =>
      userFields.filter(
        (field) => field.name.toLowerCase().includes(searchQuery.toLowerCase()) && !registeredFieldsMap.has(field.id),
      ),
    [searchQuery, registeredFieldsMap, userFields],
  );

  const allRegisteredFieldsList = useMemo(
    () => [...registrationHistory].sort((a, b) => a.fieldName.localeCompare(b.fieldName)),
    [registrationHistory],
  );

  const filteredCrops = useMemo(
    () => {
      const source = cropOptions.length > 0
        ? cropOptions
        : CROP_DATA.LIST.map((name) => ({ uuid: '', name }));
      return source.filter((option) => option.name.toLowerCase().includes(cropSearchQuery.toLowerCase()));
    },
    [cropOptions, cropSearchQuery],
  );

  const cropNameToUuid = useMemo(() => {
    const map = new Map<string, string>();
    cropOptions.forEach((option) => {
      if (option.uuid) {
        map.set(option.name, option.uuid);
      }
    });
    return map;
  }, [cropOptions]);

  const {
    paginatedData: paginatedFilteredFields,
    PaginationControls: UnregisteredPagination,
    setCurrentPage,
  } = usePagination(filteredFields, 10);
  const { paginatedData: paginatedRegisteredFields, PaginationControls: RegisteredPagination } = usePagination(
    allRegisteredFieldsList,
    5,
  );

  useEffect(() => {
    setCurrentPage(1);
  }, [searchQuery, setCurrentPage]);

  const isStep2Complete = useMemo(() => {
    if (!cropName) return false;
    const {
      cropUuid,
      variety,
      varietyUuid,
      yield: targetYield,
      growth_stage,
      previous_crop,
      planting_method,
      tillageUuid,
      seedingTillageSystemUuid,
    } = templateValues;
    if (!cropUuid || !variety || !varietyUuid || !targetYield || !growth_stage || !previous_crop) return false;
    if (!Number.isFinite(Number(targetYield))) return false;
    if (CROP_DATA.NEEDS_PLANTING_METHOD.includes(cropName) && !planting_method) return false;
    if (needsTillage(cropName) && !tillageUuid) return false;
    if (needsSeedingTillageSystem(cropName) && !seedingTillageSystemUuid) return false;
    if (previous_crop && previous_crop !== '該当なし' && !templateValues.previousCropUuid) return false;
    return true;
  }, [cropName, templateValues]);

  const hasVarietyLookupData = varietyOptions.length > 0;
  const filteredVarietyOptions = useMemo(() => {
    if (!varietyOptions.length) return [];
    const query = varietySearchQuery.trim().toLowerCase();
    let filtered = query
      ? varietyOptions.filter((opt) => {
          const combined = `${opt.name ?? ''} ${opt.code ?? ''} ${opt.registrationNumber ?? ''}`.toLowerCase();
          return combined.includes(query);
        })
      : varietyOptions.slice();
    const currentUuid = templateValues.varietyUuid;
    if (currentUuid) {
      const currentOption = varietyOptions.find((opt) => opt.uuid === currentUuid);
      if (currentOption && !filtered.some((opt) => opt.uuid === currentOption.uuid)) {
        filtered = [currentOption, ...filtered];
      }
    }
    return filtered;
  }, [varietyOptions, varietySearchQuery, templateValues.varietyUuid]);

  const handleToggleSelection = (fieldId: string) => {
    setSelectedFieldIds((prev) => {
      const next = new Set(prev);
      if (next.has(fieldId)) {
        next.delete(fieldId);
      } else {
        next.add(fieldId);
      }
      return next;
    });
  };

  const handleSelectAllOnPage = (e: ChangeEvent<HTMLInputElement>) => {
    const isChecked = e.target.checked;
    setSelectedFieldIds((prev) => {
      const next = new Set(prev);
      paginatedFilteredFields.forEach((field) => {
        if (isChecked) next.add(field.id);
        else next.delete(field.id);
      });
      return next;
    });
  };

  const handleDateSelectAndRegister = (date: string) => {
    const effectiveCropUuid = templateValues.cropUuid || selectedCrop?.uuid || '';
    if (!effectiveCropUuid) {
      window.alert('作物が選択されていません。作物を選択してから再度作付日を選択してください。');
      return;
    }
    if (!templateValues.varietyUuid) {
      window.alert('品種が選択されていません。品種を選択してから再度作付日を選択してください。');
      return;
    }
    if (needsTillage(cropName) && !templateValues.tillageUuid) {
      window.alert('耕起が選択されていません。耕起を選択してから再度作付日を選択してください。');
      return;
    }
    if (needsSeedingTillageSystem(cropName) && !templateValues.seedingTillageSystemUuid) {
      window.alert('播種方式が選択されていません。播種方式を選択してから再度作付日を選択してください。');
      return;
    }
    if (!Number.isFinite(Number(templateValues.yield))) {
      window.alert('予想収量には数値を入力してください。');
      return;
    }
    const selectedDate = new Date(`${date}T00:00:00Z`);
    const today = new Date();
    today.setUTCHours(0, 0, 0, 0);
    const earliestAllowed = new Date(today);
    earliestAllowed.setUTCDate(earliestAllowed.getUTCDate() - 200);
    if (selectedDate < earliestAllowed) {
      window.alert('作付日は過去200日以内の日付のみ選択できます。');
      return;
    }
    const registrations: FormData[] = [];
    selectedFieldIds.forEach((id) => {
      const field = userFields.find((f) => f.id === id);
      if (!field) return;
      registrations.push({
        fieldId: id,
        fieldName: field.name,
        crop_name: cropName,
        cropUuid: effectiveCropUuid,
        planting_method: templateValues.planting_method || '',
        growth_stage: templateValues.growth_stage || '',
        planting_date: date,
        variety: templateValues.variety || '',
        varietyUuid: templateValues.varietyUuid || '',
        yield: templateValues.yield || '',
        previous_crop: templateValues.previous_crop || '該当なし',
        previousCropUuid:
          templateValues.previousCropUuid ||
          (templateValues.previous_crop && templateValues.previous_crop !== '該当なし'
            ? cropNameToUuid.get(templateValues.previous_crop) ?? ''
            : ''),
        tillage: templateValues.tillage || '',
        tillageUuid: templateValues.tillageUuid || '',
        seedingTillageSystemUuid: templateValues.seedingTillageSystemUuid || '',
        seedingTillageSystemName: templateValues.seedingTillageSystemName || '',
        prefecture: field.prefecture || '',
        municipality: field.municipality || '',
      });
    });
    if (registrations.length > 0) {
      const requestPayload: CropSeasonCreatePayload[] = [];
      for (const reg of registrations) {
        const expectedYieldNumber = Number(reg.yield);
        const startDateIso = (() => {
          if (!reg.planting_date) return null;
          const date = new Date(`${reg.planting_date}T00:00:00+09:00`);
          return Number.isNaN(date.getTime()) ? null : date.toISOString();
        })();

        if (!startDateIso) {
          window.alert('作付日の変換に失敗しました。別の日付を選択してください。');
          return;
        }

        const yieldExpectation =
          Number.isFinite(expectedYieldNumber) && expectedYieldNumber >= 0
            ? Number(expectedYieldNumber.toFixed(4))
            : 0;

        const payload: CropSeasonCreatePayload = {
          fieldUuid: reg.fieldId,
          cropUuid: reg.cropUuid,
          varietyUuid: reg.varietyUuid,
          startDate: startDateIso,
          yieldExpectation,
        };

        requestPayload.push(payload);
      }

      setPendingRegistrations(registrations);
      setPendingPayloads(requestPayload);
      setSubmissionError(null);
      setSubmissionLoading(false);
      setIsConfirmationOpen(true);
    }
  };

  const handleCancelConfirmation = () => {
    if (submissionLoading) return;
    setIsConfirmationOpen(false);
    setPendingRegistrations(null);
    setPendingPayloads([]);
    setSubmissionError(null);
  };

  const handleSubmitCropSeasons = async () => {
    if (!pendingRegistrations || pendingRegistrations.length === 0) {
      setSubmissionError('登録する作付情報が選択されていません。');
      return;
    }
    if (!auth) {
      setSubmissionError('認証情報が見つかりません。再度ログインしてください。');
      return;
    }
    if (pendingPayloads.length !== pendingRegistrations.length) {
      setSubmissionError('確認情報と作付データの件数が一致しません。最初からやり直してください。');
      return;
    }
    setSubmissionLoading(true);
    setSubmissionError(null);
    try {
      const response = await fetch(withApiBase('/crop-seasons'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          login_token: auth.login.login_token,
          api_token: auth.api_token,
          payloads: pendingPayloads,
        }),
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`HTTP ${response.status}: ${text.slice(0, 200)}`);
      }
      const appliedRegistrations = pendingRegistrations;
      setIsConfirmationOpen(false);
      setPendingRegistrations(null);
      setPendingPayloads([]);
      setSubmissionLoading(false);
      onRegister(appliedRegistrations);
    } catch (error) {
      const message = error instanceof Error ? error.message : '作付登録の送信に失敗しました。';
      setSubmissionError(message);
      setSubmissionLoading(false);
    }
  };

  const stepTitles = ['ステップ1/4：作物の選択', 'ステップ2/4：作付情報の入力', 'ステップ3/4：圃場の選択', 'ステップ4/4：作付日の選択'];
  const allOnPageSelected = paginatedFilteredFields.length > 0 && paginatedFilteredFields.every((field) => selectedFieldIds.has(field.id));

  return (
    <div className="details-screen">
      <div className="details-header">
        <h2>{stepTitles[step - 1]}</h2>
        <button onClick={onCancel} className="cancel-button">
          閉じる
        </button>
      </div>

      <div className="wizard-content">
        {step === 1 && (
          <div className="wizard-step">
            <div className="wizard-step-body crop-selection-body">
              <input
                type="search"
                placeholder="作物を検索..."
                value={cropSearchQuery}
                onChange={(e) => setCropSearchQuery(e.target.value)}
                className="crop-search-input"
                aria-label="作物を検索"
              />
              <div className="crop-selection-grid">
                {cropsLoading && <div className="crop-list-empty">作物を取得しています...</div>}
                {cropFetchError && <div className="crop-list-empty" style={{ color: '#ff6b6b' }}>{cropFetchError}</div>}
                {!cropsLoading && filteredCrops.length > 0 ? (
                  filteredCrops.map((option) => (
                    <div
                      key={option.uuid || option.name}
                      className="crop-tile"
                      onClick={() => {
                        setSelectedCrop(option);
                        setVarietyOptions([]);
                        setVarietiesError(null);
                        setVarietiesLoading(false);
                        setVarietySearchQuery('');
                        setTemplateValues((prev) => ({
                          ...prev,
                          cropUuid: option.uuid,
                        }));
                        setCropName(option.name);
                        setStep(2);
                      }}
                      role="button"
                      tabIndex={0}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter') {
                          setSelectedCrop(option);
                          setVarietyOptions([]);
                          setVarietiesError(null);
                          setVarietiesLoading(false);
                          setVarietySearchQuery('');
                            setTemplateValues((prev) => ({
                              ...prev,
                              cropUuid: option.uuid,
                            }));
                            setCropName(option.name);
                            setStep(2);
                          }
                        }}
                    >
                      <span className="crop-icon" aria-hidden="true">
                        {CROP_DATA.ICONS[option.name] || '🌱'}
                      </span>
                      <span>{option.name}</span>
                      {option.uuid && <span className="crop-subtext">UUID: {option.uuid}</span>}
                    </div>
                  ))
                ) : (!cropsLoading && filteredCrops.length === 0 ? (
                  <div className="crop-list-empty">該当する作物がありません。</div>
                ) : null)}
              </div>
            </div>
          </div>
        )}
        {step === 2 && (
          <div className="wizard-step">
            <div className="wizard-step-body">
              <CropFormFields
                formData={templateValues}
                onFormChange={(e) => {
                  const { name, value } = e.target;
                  setTemplateValues((prev) => ({
                    ...prev,
                    [name]: value,
                    ...(name === 'previous_crop'
                      ? {
                          previousCropUuid:
                            value && value !== '該当なし' ? cropNameToUuid.get(value) ?? '' : '',
                        }
                      : {}),
                  }));
                }}
                cropName={cropName}
                isEditMode={false}
                varietyOptions={filteredVarietyOptions}
                varietiesLoading={varietiesLoading}
                varietiesError={varietiesError}
                onSelectVariety={(option) =>
                  setTemplateValues((prev) => ({
                    ...prev,
                    variety: option?.name ?? '',
                    varietyUuid: option?.uuid ?? '',
                  }))
                }
                varietySearchEnabled
                varietySearchQuery={varietySearchQuery}
                onVarietySearchChange={setVarietySearchQuery}
                hasVarietyLookupData={hasVarietyLookupData}
                tillageOptions={tillageSystems}
                tillageLoading={tillageSystemsLoading}
                tillageError={tillageSystemsError}
                onSelectTillage={handleSelectTillage}
                seedingTillageSystemOptions={tillageSystems}
                seedingTillageSystemLoading={tillageSystemsLoading}
                seedingTillageSystemError={tillageSystemsError}
                onSelectSeedingTillageSystem={handleSelectSeedingTillageSystem}
              />
            </div>
            <div className="wizard-nav">
              <button className="wizard-nav-button secondary" onClick={() => setStep(1)}>
                戻る
              </button>
              <button
                className="wizard-nav-button"
                onClick={() => {
                  if (isStep2Complete) setStep(3);
                  else window.alert('必須項目をすべて入力してください。');
                }}
                disabled={!isStep2Complete}
              >
                次へ: 圃場を選択
              </button>
            </div>
          </div>
        )}
        {step === 3 && (
          <div className="wizard-step">
            <div className="wizard-step-body">
              <input
                type="search"
                placeholder="圃場名で検索..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="field-search-input"
                aria-label="圃場を検索"
              />
              <div className="field-list-header">
                <div className="field-list-header-group">
                  <input
                    type="checkbox"
                    id="select-all-on-page"
                    checked={allOnPageSelected}
                    onChange={handleSelectAllOnPage}
                    disabled={paginatedFilteredFields.length === 0}
                  />
                  <label htmlFor="select-all-on-page">このページの未登録圃場</label>
                </div>
                {selectedFieldIds.size > 0 && <span className="selected-count-badge">{selectedFieldIds.size}件 選択中</span>}
              </div>
              <div className="field-card-grid">
                {paginatedFilteredFields.length > 0 ? (
                  paginatedFilteredFields.map((field) => {
                    const isSelected = selectedFieldIds.has(field.id);
                    return (
                      <div
                        key={field.id}
                        className={`field-card ${isSelected ? 'selected' : ''}`}
                        onClick={() => handleToggleSelection(field.id)}
                        role="button"
                      >
                        <input type="checkbox" checked={isSelected} readOnly />
                        <label className="field-name">{field.name}</label>
                      </div>
                    );
                  })
                ) : (
                  <div className="field-card-empty">検索条件に合う未登録の圃場はありません。</div>
                )}
              </div>
              <UnregisteredPagination />
              {allRegisteredFieldsList.length > 0 && (
                <div className="registered-fields-container">
                  <div className="registered-header" onClick={() => setIsRegisteredListCollapsed((p) => !p)} role="button">
                    <h4>登録済み ({allRegisteredFieldsList.length})</h4>
                    <button className="collapse-toggle-button" aria-expanded={!isRegisteredListCollapsed}>
                      {isRegisteredListCollapsed ? '＋' : '−'}
                    </button>
                  </div>
                  <div className={`collapsible-content ${isRegisteredListCollapsed ? 'collapsed' : ''}`}>
                    <div className="field-card-grid">
                      {paginatedRegisteredFields.map((field) => (
                        <div key={field.fieldId} className="field-card registered">
                          <div className="field-info">
                            <span className="field-name">{field.fieldName}</span>
                            <div className="field-details">
                              <span>
                                {field.crop_name} ({field.variety})
                              </span>
                              <span>
                                {new Date(`${field.planting_date}T00:00:00Z`).toLocaleDateString('ja-JP', {
                                  timeZone: 'UTC',
                                  month: 'short',
                                  day: 'numeric',
                                })}
                              </span>
                            </div>
                          </div>
                          <button
                            onClick={() => onUnregister(field.fieldId)}
                            className="cancel-registration-button"
                            title={`「${field.fieldName}」の登録を取り消す`}
                          >
                            &times;
                          </button>
                        </div>
                      ))}
                    </div>
                    <RegisteredPagination className="registered-pagination" />
                  </div>
                </div>
              )}
            </div>
            <div className="wizard-nav">
              <button className="wizard-nav-button secondary" onClick={() => setStep(2)}>
                戻る
              </button>
              <button
                className="wizard-nav-button"
                onClick={() => {
                  if (selectedFieldIds.size > 0) setStep(4);
                  else window.alert('作付けする圃場を1つ以上選択してください。');
                }}
                disabled={selectedFieldIds.size === 0}
              >
                次へ: 作付日を選択
              </button>
            </div>
          </div>
        )}
        {step === 4 && (
          <div className="wizard-step">
            <div className="wizard-step-body calendar-step-body">
              <p className="calendar-selection-header">
                {selectedFieldIds.size}
                件の圃場が選択されています。カレンダーから作付日をクリックして登録を完了してください。
              </p>
              <Calendar
                currentDate={calendarDate}
                setCurrentDate={setCalendarDate}
                onDateClick={handleDateSelectAndRegister}
                selectionMode
              />
            </div>
            <div className="wizard-nav">
              <button className="wizard-nav-button secondary" onClick={() => setStep(3)}>
                戻る
              </button>
            </div>
          </div>
        )}
      </div>
      <Modal isOpen={isConfirmationOpen} className="registration-confirmation-modal">
        <div className="registration-confirmation-dialog">
          <div className="registration-confirmation-header">
            <h3>作付登録の確認</h3>
            <p>
              {pendingRegistrations?.length ?? 0}件の作付登録を作成します。内容を確認してから「作付登録を確定」を押してください。
            </p>
          </div>
          <div className="registration-confirmation-table-wrapper">
            <table className="registration-confirmation-table">
              <thead>
                <tr>
                  <th>圃場</th>
                  <th>作物 / 品種</th>
                  <th>作付日</th>
                  <th>作付方法</th>
                  <th>生育ステージ</th>
                  <th>予想収量</th>
                  <th>ライフサイクル</th>
                  <th>前作</th>
                </tr>
              </thead>
              <tbody>
                {(pendingRegistrations ?? []).map((reg, idx) => {
                  const payload = pendingPayloads[idx];
                  const startDateLabel = payload?.startDate
                    ? new Date(payload.startDate).toLocaleDateString('ja-JP', {
                        year: 'numeric',
                        month: 'short',
                        day: 'numeric',
                      })
                    : new Date(`${reg.planting_date}T00:00:00Z`).toLocaleDateString('ja-JP', {
                        year: 'numeric',
                        month: 'short',
                        day: 'numeric',
                      });
                  const yieldLabel = Number.isFinite(Number(reg.yield))
                    ? reg.yield
                    : Number(payload?.yieldExpectation ?? 0).toLocaleString('ja-JP', {
                        maximumFractionDigits: 3,
                      });
                  const lifecycleDisplay = (() => {
                    const plantingDateUtc = new Date(`${reg.planting_date}T00:00:00Z`);
                    const today = new Date();
                    today.setUTCHours(0, 0, 0, 0);
                    return plantingDateUtc > today ? 'PLANNED' : 'ACTIVE';
                  })();
                  return (
                    <tr key={`${reg.fieldId}-${idx}`}>
                      <td>
                        <div className="confirmation-field-name">{reg.fieldName}</div>
                        <div className="confirmation-field-location">
                          {[reg.prefecture, reg.municipality].filter(Boolean).join(' ')}
                        </div>
                      </td>
                      <td>
                        <div>{reg.crop_name}</div>
                        <div className="confirmation-subtext">{reg.variety}</div>
                      </td>
                      <td>{startDateLabel}</td>
                      <td>{reg.planting_method || '-'}</td>
                      <td>{payload?.cropEstablishmentGrowthStageIndex || reg.growth_stage || '-'}</td>
                      <td>{yieldLabel}</td>
                    <td>{lifecycleDisplay}</td>
                      <td>{reg.previous_crop}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          {submissionError && <div className="registration-confirmation-error">{submissionError}</div>}
          <div className="registration-confirmation-actions">
            <button
              type="button"
              className="wizard-nav-button secondary"
              onClick={handleCancelConfirmation}
              disabled={submissionLoading}
            >
              戻る
            </button>
            <button
              type="button"
              className="wizard-nav-button"
              onClick={handleSubmitCropSeasons}
              disabled={submissionLoading}
            >
              {submissionLoading ? '登録中...' : '作付登録を確定'}
            </button>
          </div>
        </div>
      </Modal>
    </div>
  );
};

const parseCombinedFields = (fields: CombinedFieldResponse[]) => {
  const userFieldMap = new Map<string, UserField>();
  fields.forEach((field) => {
    if (!field?.uuid) return;
    const areaValue = typeof field.area === 'number' ? field.area : 0;
    const areaHa = (areaValue / 10000).toFixed(1);
    const label = field.name ? `${field.name} (${areaHa}ha)` : `${field.uuid} (${areaHa}ha)`;
    const location = (field as any).location || {};
    userFieldMap.set(field.uuid, {
      id: field.uuid,
      name: label,
      prefecture: location.prefecture ?? '',
      municipality: location.municipality ?? '',
    });
  });
  const userFields = Array.from(userFieldMap.values()).sort((a, b) => a.name.localeCompare(b.name));

  const methodMap: Record<string, string> = {
    TRANSPLANTING: '移植',
    DIRECT_SEEDING: '直播',
    MYKOS_DRY_DIRECT_SEEDING: '節水型乾田直播',
  };

  const initialRegistrations: FormData[] = [];
  fields.forEach((field) => {
    if (!field?.uuid) return;
    const displayName = userFieldMap.get(field.uuid)?.name || field.name || field.uuid;
    const locationMeta = userFieldMap.get(field.uuid);
    const fieldUuid = field.uuid;
    field.cropSeasonsV2?.forEach((season) => {
      if (!season || season.lifecycleState !== 'ACTIVE') return;
      const cropName = season.crop?.name ?? '';
      const varietyName = season.variety?.name ?? '';
      const methodKey = season.cropEstablishmentMethodCode ?? '';
      const plantingMethod = methodMap[methodKey] ?? '';
      const rawStage = season.cropEstablishmentGrowthStageIndex;
      let growthStage = 'BBCH0';
      if (rawStage) {
        const rawString = String(rawStage).toUpperCase();
        growthStage = rawString.startsWith('BBCH') ? rawString : `BBCH${rawString}`;
      }
      const plantingDate = season.startDate ? season.startDate.split('T')[0] : '';
      const cropUuid = season.crop?.uuid ?? '';
      const varietyUuid = season.variety?.uuid ?? '';
      const preCropData = (season as any)?.preCrop;
      const preCropUuidRaw =
        (typeof season.preCropUuid === 'string' ? season.preCropUuid : '') || preCropData?.uuid || '';
      const preCropUuid = preCropUuidRaw || '';
      const preCropName = preCropUuid ? preCropData?.name ?? '情報なし' : '該当なし';
      initialRegistrations.push({
        fieldId: fieldUuid,
        fieldName: displayName,
        crop_name: cropName,
        cropUuid,
        variety: varietyName,
        varietyUuid,
        planting_method: plantingMethod,
        growth_stage: growthStage,
        planting_date: plantingDate,
        yield: '',
        previous_crop: preCropName,
        previousCropUuid: preCropUuid,
        tillage: '',
        tillageUuid: '',
        seedingTillageSystemUuid: '',
        seedingTillageSystemName: '',
        prefecture: locationMeta?.prefecture ?? '',
        municipality: locationMeta?.municipality ?? '',
      });
    });
  });

  return { userFields, initialRegistrations };
};

export const CropRegistrationPage: FC = () => {
  const {
    combinedOut,
    combinedLoading,
    combinedErr,
    combinedFetchAttempt,
    combinedFetchMaxAttempts,
    combinedRetryCountdown,
  } = useData();
  const { submittedFarms, fetchCombinedDataIfNeeded } = useFarms();
  const { auth } = useAuth();
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isCalendarViewOpen, setIsCalendarViewOpen] = useState(false);
  const [registrationHistory, setRegistrationHistory] = useState<FormData[]>([]);
  const [userFields, setUserFields] = useState<UserField[]>([]);
  const [hasInitialized, setHasInitialized] = useState(false);
  const [tillageSystems, setTillageSystems] = useState<TillageOption[]>([]);
  const [tillageSystemsLoading, setTillageSystemsLoading] = useState(false);
  const [tillageSystemsError, setTillageSystemsError] = useState<string | null>(null);
  const tillageSystemsFetched = useRef(false);

  useEffect(() => {
    fetchCombinedDataIfNeeded();
  }, [fetchCombinedDataIfNeeded]);

  useEffect(() => {
    tillageSystemsFetched.current = false;
    setTillageSystems([]);
    setTillageSystemsError(null);
    setTillageSystemsLoading(false);
  }, [auth?.api_token, auth?.login?.login_token]);

  const fetchTillageSystemsIfNeeded = useCallback(async () => {
    if (!auth) return;
    if (tillageSystemsFetched.current || tillageSystemsLoading) return;
    setTillageSystemsLoading(true);
    setTillageSystemsError(null);
    try {
      const { ok, status, json } = await postJsonCached<any>(
        withApiBase('/masterdata/tillage-systems'),
        {
          login_token: auth.login.login_token,
          api_token: auth.api_token,
          locale: 'JA-JP',
        },
        undefined,
        { cacheKey: 'masterdata:tillage-systems:JA-JP', cache: 'session' },
      );
      if (!ok) {
        const detail = typeof json === 'string' ? json.slice(0, 200) : '';
        throw new Error(`HTTP ${status}${detail ? `: ${detail}` : ''}`);
      }
      const items = (json.items ?? json ?? []) as any[];
      const normalized = items
        .map((item) => ({
          uuid: item?.uuid ?? '',
          name: item?.name ?? item?.code ?? '',
          code: item?.code ?? undefined,
          description: item?.description ?? item?.localizedDescription ?? undefined,
        }))
        .filter((item) => item.uuid && item.name)
        .sort((a, b) => a.name.localeCompare(b.name, 'ja'));
      setTillageSystems(normalized);
      tillageSystemsFetched.current = true;
    } catch (error) {
      const message = error instanceof Error ? error.message : '播種方式の取得に失敗しました';
      setTillageSystemsError(message);
      setTillageSystems([]);
      tillageSystemsFetched.current = false;
    } finally {
      setTillageSystemsLoading(false);
    }
  }, [auth]);


  const farmsKey = useMemo(() => [...submittedFarms].sort().join(','), [submittedFarms]);

  useEffect(() => {
    setHasInitialized(false);
    setRegistrationHistory([]);
    setUserFields([]);
  }, [farmsKey]);

  const rawFields = useMemo<CombinedFieldResponse[]>(() => {
    const candidate = combinedOut?.response?.data?.fieldsV2;
    return Array.isArray(candidate) ? (candidate as CombinedFieldResponse[]) : [];
  }, [combinedOut]);

  useEffect(() => {
    if (!rawFields.length) {
      setUserFields([]);
      return;
    }
    const { userFields: parsedFields, initialRegistrations } = parseCombinedFields(rawFields);
    setUserFields(parsedFields);
    if (!hasInitialized) {
      setRegistrationHistory(initialRegistrations);
      setHasInitialized(true);
    }
  }, [rawFields, hasInitialized]);

  const handleRegistrationComplete = (data: FormData[]) => {
    if (data.length > 0) {
      setRegistrationHistory((prevHistory) => {
        const historyMap = new Map(prevHistory.map((item) => [item.fieldId, item]));
        data.forEach((newItem) => historyMap.set(newItem.fieldId, newItem));
        return Array.from(historyMap.values());
      });
    }
    setIsModalOpen(false);
  };

  const handleUnregister = (fieldIdToCancel: string) => {
    setRegistrationHistory((prev) => prev.filter((reg) => reg.fieldId !== fieldIdToCancel));
  };

  const handleUpdateRegistration = (updatedData: FormData) => {
    setRegistrationHistory((prev) => prev.map((reg) => (reg.fieldId === updatedData.fieldId ? updatedData : reg)));
  };

  const loadingMessage = formatCombinedLoadingMessage(
    '作付データ',
    combinedFetchAttempt,
    combinedFetchMaxAttempts,
    combinedRetryCountdown,
  );

  if (submittedFarms.length === 0) {
    return (
      <div className="registration-app-container">
        <header className="registration-header">
          <h1>CropSeason Creator（工事中）</h1>
        </header>
        <main className="registration-home-content">
          <div className="home-card">
            <h2 className="home-title">対象の農場を選択してください</h2>
            <p className="home-subtitle">ヘッダーのドロップダウンから農場を選択すると、作付登録を開始できます。</p>
          </div>
        </main>
      </div>
    );
  }

  if (combinedErr) {
    return (
      <div className="registration-app-container">
        <header className="registration-header">
          <h1>CropSeason Creator（工事中）</h1>
        </header>
        <main className="registration-home-content">
          <div className="home-card">
            <h2 className="home-title">作付データの取得に失敗しました</h2>
            <p className="home-subtitle" style={{ color: '#ff6b6b' }}>
              {combinedErr}
            </p>
          </div>
        </main>
      </div>
    );
  }

  return (
    <div className="registration-app-container">
      {combinedLoading && <LoadingOverlay message={loadingMessage} />}
      <header className="registration-header">
        <h1>CropSeason Creator（工事中）</h1>
      </header>
      <main className="registration-home-content">
        <div className="home-card">
          <h2 className="home-title">営農支援へようこそ</h2>
          <p className="home-subtitle">作付の計画と管理を始めましょう</p>
          {!userFields.length && !combinedLoading && (
            <p className="home-subtitle" style={{ color: '#a0a0ab' }}>
              利用可能な圃場がありません。作付登録やカレンダーの表示はできません。
            </p>
          )}
          <div className="home-actions">
            <button
              className="home-button"
              onClick={() => setIsModalOpen(true)}
              disabled={combinedLoading || userFields.length === 0}
            >
              <span className="button-icon" aria-hidden="true">
                📝
              </span>
              <span className="button-text">
                <span className="button-title">作付登録</span>
                <span className="button-description">新しい作付計画を登録します</span>
              </span>
            </button>
            <button
              className="home-button"
              onClick={() => setIsCalendarViewOpen(true)}
              disabled={combinedLoading || registrationHistory.length === 0}
            >
              <span className="button-icon" aria-hidden="true">
                🗓️
              </span>
              <span className="button-text">
                <span className="button-title">登録の確認・編集</span>
                <span className="button-description">カレンダーで計画を管理します</span>
              </span>
            </button>
          </div>
        </div>
      </main>

      <Modal isOpen={isModalOpen} className="registration-modal">
        <RegistrationDetailsScreen
          onRegister={handleRegistrationComplete}
          onCancel={() => setIsModalOpen(false)}
          registrationHistory={registrationHistory}
          onUnregister={handleUnregister}
          userFields={userFields}
          tillageSystems={tillageSystems}
          tillageSystemsLoading={tillageSystemsLoading}
          tillageSystemsError={tillageSystemsError}
          onEnsureTillageSystems={fetchTillageSystemsIfNeeded}
        />
      </Modal>
      <Modal isOpen={isCalendarViewOpen} className="calendar-view-modal">
        <CalendarViewScreen
          registrationHistory={registrationHistory}
          onClose={() => setIsCalendarViewOpen(false)}
          onUpdateRegistration={handleUpdateRegistration}
          onUnregister={handleUnregister}
          tillageSystems={tillageSystems}
          tillageSystemsLoading={tillageSystemsLoading}
          tillageSystemsError={tillageSystemsError}
          onEnsureTillageSystems={fetchTillageSystemsIfNeeded}
        />
      </Modal>
    </div>
  );
};
