import { Routes, Route, Navigate, Outlet } from 'react-router-dom';
import { AuthProvider, useAuth } from './context/AuthContext';
import { FarmProvider } from './context/FarmContext';
import { DataProvider } from './context/DataContext';
import { WarmupProvider } from './context/WarmupContext';
import { Layout } from './components/Layout';
import { LoginForm } from './components/LoginForm';
import { FarmsPage } from './pages/FarmsPage';
import { TasksPage } from './pages/TasksPage'; // 既存のインポート
import { FieldMemoPage } from './pages/FieldMemoPage';
import { RiskPage } from './pages/RiskPage';
import { GrowthStagePredictionPage } from './pages/GrowthStagePredictionPage';
import { SprayingWeatherPage } from './pages/SprayingWeatherPage';
import { WeatherSelectionPage } from './pages/WeatherSelectionPage';
import { NdviPage } from './pages/NdviPage';
import { SatelliteMapPage } from './pages/SatelliteMapPage';
import { CropRegistrationPage } from './pages/CropRegistrationPage';
import { WarmupToast } from './components/WarmupToast';
import { CombinedDataToast } from './components/CombinedDataToast';

/**
 * 認証が必要なルートを保護するコンポーネント。
 * 認証されていない場合はログインページにリダイレクトします。
 * 認証されている場合は、子ルートをレンダリングします。
 */
function ProtectedRoute() {
  const { auth } = useAuth();
  // 認証されていれば <Outlet /> をレンダリングし、子ルートを表示
  return auth ? <Outlet /> : <Navigate to="/login" replace />;
}

function AppRoutes() {
  return (
    <Routes>
      <Route path="/login" element={<LoginForm />} />
      <Route element={<Layout />}> {/* 全ての保護されたページで共通のレイアウトを適用 */}
        <Route path="/" element={<ProtectedRoute />}>
          <Route path="/farms" element={<FarmsPage />} />
          <Route path="/tasks" element={<TasksPage />} />
          <Route path="/ndvi" element={<NdviPage />} />
          <Route path="/satellite-map" element={<SatelliteMapPage />} />
          <Route path="crop-registration" element={<CropRegistrationPage />} />
          <Route path="field-memo" element={<FieldMemoPage />} />
          <Route path="risks" element={<RiskPage />} />
          <Route path="growth-stage-predictions" element={<GrowthStagePredictionPage />} />
          <Route path="weather" element={<WeatherSelectionPage />} />
          <Route path="weather/:fieldUuid" element={<SprayingWeatherPage />} />
          <Route path="/" element={<Navigate to="/farms" replace />} />
        </Route>
      </Route>
    </Routes>
  );
}

export default function App() {
  return (
    <AuthProvider>
      <WarmupProvider>
        <DataProvider>
          <FarmProvider>
            <WarmupToast />
            <CombinedDataToast />
            <AppRoutes />
          </FarmProvider>
        </DataProvider>
      </WarmupProvider>
    </AuthProvider>
  );
}
