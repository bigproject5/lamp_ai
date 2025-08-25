import cv2
import numpy as np
from typing import Dict, Any, Optional

class TechnicalBrightnessAnalyzer:
    def __init__(self):
        self.brightness_standards = {
            'headlight_on': {
                'target_brightness': 200,
                'min_brightness': 150,
                'max_brightness': 250,
            },
            'taillight_on': {
                'target_brightness': 120,
                'min_brightness': 80,
                'max_brightness': 160,
            }
        }
        self.uniformity_standards = {
            'cv_threshold': 0.25,
            'percentile_ratio': 3.0,
            'spatial_uniformity': 0.8,
        }

    def calculate_coefficient_of_variation(self, gray_roi: np.ndarray) -> Dict[str, Any]:
        """변동계수 계산"""
        mean_val = np.mean(gray_roi)
        std_val = np.std(gray_roi)

        if mean_val == 0:
            return {'cv': float('inf'), 'grade': 'D급 (불량)', 'pass': False}

        cv = std_val / mean_val

        return {
            'cv': cv,
            'grade': self._grade_cv(cv),
            'pass': cv <= self.uniformity_standards['cv_threshold']
        }

    def _grade_cv(self, cv: float) -> str:
        """변동계수 등급 평가"""
        if cv <= 0.15:
            return 'A급 (우수)'
        elif cv <= 0.25:
            return 'B급 (양호)'
        elif cv <= 0.35:
            return 'C급 (보통)'
        else:
            return 'D급 (불량)'

    def calculate_percentile_uniformity(self, gray_roi: np.ndarray) -> Dict[str, Any]:
        """백분위수 균일도 계산"""
        p10 = np.percentile(gray_roi, 10)
        p90 = np.percentile(gray_roi, 90)

        if p10 == 0:
            ratio = float('inf')
        else:
            ratio = p90 / p10

        return {
            'p90_p10_ratio': ratio,
            'uniformity_grade': self._grade_percentile_ratio(ratio),
            'pass': ratio <= self.uniformity_standards['percentile_ratio']
        }

    def _grade_percentile_ratio(self, ratio: float) -> str:
        """백분위수 비율 등급 평가"""
        if ratio <= 2.0:
            return 'A급 (매우 균일)'
        elif ratio <= 3.0:
            return 'B급 (균일)'
        elif ratio <= 4.0:
            return 'C급 (보통)'
        elif ratio <= 5.0:
            return 'D급 (불균일)'
        else:
            return 'F급 (매우 불균일)'

    def calculate_spatial_uniformity(self, gray_roi: np.ndarray) -> Dict[str, Any]:
        """공간적 균일도 계산"""
        h, w = gray_roi.shape
        grid_size = 3
        cell_h, cell_w = h // grid_size, w // grid_size
        overall_mean = np.mean(gray_roi)

        compliant_cells = 0
        tolerance = 0.2

        for i in range(grid_size):
            for j in range(grid_size):
                y1, y2 = i * cell_h, min((i + 1) * cell_h, h)
                x1, x2 = j * cell_w, min((j + 1) * cell_w, w)

                cell = gray_roi[y1:y2, x1:x2]
                cell_mean = np.mean(cell)

                if overall_mean > 0:
                    deviation = abs(cell_mean - overall_mean) / overall_mean
                    if deviation <= tolerance:
                        compliant_cells += 1

        total_cells = grid_size * grid_size
        spatial_score = compliant_cells / total_cells

        return {
            'spatial_uniformity_score': spatial_score,
            'compliant_cells': compliant_cells,
            'total_cells': total_cells,
            'grade': self._grade_spatial_uniformity(spatial_score),
            'pass': spatial_score >= self.uniformity_standards['spatial_uniformity']
        }

    def _grade_spatial_uniformity(self, score: float) -> str:
        """공간적 균일도 등급 평가"""
        if score >= 0.9:
            return 'A급 (매우 균일)'
        elif score >= 0.8:
            return 'B급 (균일)'
        elif score >= 0.7:
            return 'C급 (보통)'
        elif score >= 0.6:
            return 'D급 (불균일)'
        else:
            return 'F급 (매우 불균일)'

    def comprehensive_analysis(self, roi: np.ndarray, light_type: str) -> Optional[Dict[str, Any]]:
        """종합적인 밝기 균일도 분석"""
        if roi is None or roi.size == 0:
            return None

        if len(roi.shape) == 3:
            gray_roi = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)
        else:
            gray_roi = roi.copy()

        mean_brightness = np.mean(gray_roi)
        std_brightness = np.std(gray_roi)
        min_brightness = np.min(gray_roi)
        max_brightness = np.max(gray_roi)

        # 각종 분석
        cv_analysis = self.calculate_coefficient_of_variation(gray_roi)
        percentile_analysis = self.calculate_percentile_uniformity(gray_roi)
        spatial_analysis = self.calculate_spatial_uniformity(gray_roi)

        # 밝기 적정성
        if light_type in self.brightness_standards:
            standards = self.brightness_standards[light_type]
            brightness_adequate = (standards['min_brightness'] <= mean_brightness <= standards['max_brightness'])
        else:
            brightness_adequate = mean_brightness > 100  # 기본 임계값

        # 종합 평가
        individual_scores = {
            'brightness_adequate': brightness_adequate,
            'cv_pass': cv_analysis['pass'],
            'percentile_pass': percentile_analysis['pass'],
            'spatial_pass': spatial_analysis['pass']
        }

        # 가중 점수
        weights = {
            'brightness_adequate': 0.4,
            'cv_pass': 0.3,
            'percentile_pass': 0.2,
            'spatial_pass': 0.1
        }

        weighted_score = sum(weights[key] * (1 if individual_scores[key] else 0)
                           for key in weights.keys())

        overall_pass = weighted_score >= 0.8

        return {
            'light_type': light_type,
            'mean_brightness': round(mean_brightness, 2),
            'std_brightness': round(std_brightness, 2),
            'brightness_range': (int(min_brightness), int(max_brightness)),
            'cv_analysis': cv_analysis,
            'percentile_analysis': percentile_analysis,
            'spatial_analysis': spatial_analysis,
            'individual_scores': individual_scores,
            'weighted_score': round(weighted_score, 3),
            'overall_pass': overall_pass,
            'technical_summary': self._generate_technical_summary(individual_scores, cv_analysis, percentile_analysis)
        }

    def _generate_technical_summary(self, scores: Dict[str, bool], cv_analysis: Dict, percentile_analysis: Dict) -> list:
        """요약 생성"""
        summary = []

        if not scores['brightness_adequate']:
            summary.append("밝기 부적절 (규정 범위 벗어남)")
        if not scores['cv_pass']:
            summary.append(f"변동계수 과대 (CV={cv_analysis['cv']:.3f}, 기준<0.25)")
        if not scores['percentile_pass']:
            summary.append(f"분포 불균일 (P90/P10={percentile_analysis['p90_p10_ratio']:.1f}, 기준<3.0)")
        if not scores['spatial_pass']:
            summary.append("공간적 불균일 (격자 분석 기준 미달)")

        if not summary:
            summary.append("모든 기준 만족")

        return summary
