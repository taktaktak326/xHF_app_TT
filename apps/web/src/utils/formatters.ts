/**
 * 日付文字列またはDateオブジェクトを 'YYYY/MM/DD' (JST) 形式の文字列に変換します。
 * @param dateString - 変換する日付
 * @returns フォーマットされた日付文字列。無効な入力の場合は空文字列を返します。
 */
export function getLocalDateString(dateString: string | Date | null | undefined): string {
  if (!dateString) return '';
  try {
    return new Date(dateString).toLocaleDateString('ja-JP', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      timeZone: 'Asia/Tokyo',
    });
  } catch (e) {
    console.error('Invalid date for getLocalDateString:', dateString);
    return '';
  }
}

/**
 * APIからの終了日は排他的（その日の00:00）な場合があるため、
 * 表示上は1日前の日付としてフォーマットします。
 * @param dateString - 変換する終了日
 * @returns フォーマットされた日付文字列。
 */
export function formatInclusiveEndDate(dateString: string | Date | null | undefined): string {
  if (!dateString) return '';
  try {
    const date = new Date(dateString);
    // 終了日の前日を計算して表示
    date.setDate(date.getDate() - 1);
    return getLocalDateString(date);
  } catch (e) {
    console.error('Invalid date for formatInclusiveEndDate:', dateString);
    return '';
  }
}

// groupConsecutiveItemsで使われるオブジェクトの基本的な型定義
type GroupableItem = {
  startDate: string;
  endDate: string;
  status: string;
  [key: string]: any; // 他のプロパティも許容
};

/**
 * 配列内の連続するアイテムを、指定されたキーに基づいてグループ化します。
 *
 * @template T - 配列内のオブジェクトの型。
 * @param items - グループ化するオブジェクトの配列。
 * @param key - 連続性を判断するためのキー。
 * @returns グループ化され、期間が統合された新しいオブジェクトの配列。
 */
export function groupConsecutiveItems<T extends GroupableItem>(items: T[], key: keyof T): T[] {
  if (!items || items.length === 0) {
    return [];
  }

  // 開始日でソート
  const sortedItems = [...items].sort((a, b) => new Date(a.startDate).getTime() - new Date(b.startDate).getTime());

  const grouped: T[] = [];
  let currentGroup: T | null = null;

  for (const item of sortedItems) {
    if (!currentGroup) {
      currentGroup = { ...item };
      continue;
    }

    const currentEndDate = new Date(currentGroup.endDate);
    const nextStartDate = new Date(item.startDate);

    // キー（typeやuuidなど）とステータスが同じで、期間が連続しているかチェック
    if (item[key] === currentGroup[key] && item.status === currentGroup.status && currentEndDate.getTime() === nextStartDate.getTime()) {
      // 期間を延長
      currentGroup.endDate = item.endDate;
    } else {
      // 新しいグループを開始
      grouped.push(currentGroup);
      currentGroup = { ...item };
    }
  }

  if (currentGroup) {
    grouped.push(currentGroup);
  }

  return grouped;
}
