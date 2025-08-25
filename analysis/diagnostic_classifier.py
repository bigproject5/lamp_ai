import cv2
import numpy as np
from typing import Dict, Any, Tuple
from analysis.brightness_analyzer import TechnicalBrightnessAnalyzer

class DiagnosticClassifier:
    def __init__(self):
        self.brightness_analyzer = TechnicalBrightnessAnalyzer()
        self.diagnosis_mapping = {
            'NORMAL': '정상',
            'DIM_OVERALL': '전체적 어두움',
            'DIM_PARTIAL': '부분적 어두움', 
            'UNEVEN_BRIGHTNESS': '밝기 불균일',
            'HOTSPOT': '핫스팟 (과도한 밝기)',
            'FLICKERING': '깜빡임 감지',
            'OFF': '완전 소등',
            'UNKNOWN': '판단 불가'
        }

    def classify_lamp_condition(self, roi: np.ndarray, light_type: str, yolo_result: Dict) -> Dict[str, Any]:
        """램프 상태를 종합적으로 분류"""
        
        # YOLO 결과 기반 기본 분류
        yolo_class = yolo_result.get('class_name', 'unknown')
        yolo_confidence = yolo_result.get('confidence', 0.0)
        
        # OFF 상태 확인 (YOLO가 off로 분류했거나 ROI가 매우 어두운 경우)
        if 'off' in yolo_class.lower() or np.mean(roi) < 30:
            return {
                'diagnosis': 'OFF',
                'diagnosis_kr': self.diagnosis_mapping['OFF'],
                'confidence': yolo_confidence,
                'severity': 'HIGH',
                'technical_details': {'reason': 'YOLO_OFF_or_VERY_DIM'},
                'brightness_analysis': None
            }
        
        # ON 상태인 경우 상세 분석 수행
        brightness_analysis = self.brightness_analyzer.comprehensive_analysis(roi, light_type)
        
        if brightness_analysis is None:
            return {
                'diagnosis': 'UNKNOWN',
                'diagnosis_kr': self.diagnosis_mapping['UNKNOWN'],
                'confidence': 0.0,
                'severity': 'HIGH',
                'technical_details': {'reason': 'ANALYSIS_FAILED'},
                'brightness_analysis': None
            }
            
        # 상세 분류 로직
        diagnosis, severity = self._detailed_classification(brightness_analysis, roi)
        
        return {
            'diagnosis': diagnosis,
            'diagnosis_kr': self.diagnosis_mapping.get(diagnosis, '알 수 없음'),
            'confidence': max(yolo_confidence, brightness_analysis['weighted_score']),
            'severity': severity,
            'technical_details': self._extract_technical_details(brightness_analysis),
            'brightness_analysis': brightness_analysis
        }

    def _detailed_classification(self, analysis: Dict, roi: np.ndarray) -> Tuple[str, str]:
        """상세 분류 로직"""
        mean_brightness = analysis['mean_brightness']
        cv_pass = analysis['individual_scores']['cv_pass']
        percentile_pass = analysis['individual_scores']['percentile_pass']
        spatial_pass = analysis['individual_scores']['spatial_pass']
        brightness_adequate = analysis['individual_scores']['brightness_adequate']
        
        # 전체적으로 어두운 경우
        if mean_brightness < 80:
            return 'DIM_OVERALL', 'MEDIUM'
            
        # 핫스팟 감지 (매우 밝은 점이 있는 경우)
        max_brightness = np.max(roi)
        if max_brightness > 240 and analysis['percentile_analysis']['p90_p10_ratio'] > 5.0:
            return 'HOTSPOT', 'MEDIUM'
            
        # 깜빡임 감지 (표준편차가 매우 큰 경우)
        if analysis['std_brightness'] > mean_brightness * 0.4:
            return 'FLICKERING', 'HIGH'
            
        # 밝기 불균일 (CV나 공간적 균일도 문제)
        if not cv_pass or not spatial_pass:
            return 'UNEVEN_BRIGHTNESS', 'LOW'
            
        # 부분적 어두움 (백분위수 문제)
        if not percentile_pass:
            return 'DIM_PARTIAL', 'MEDIUM'
            
        # 모든 기준 통과
        if brightness_adequate and analysis['overall_pass']:
            return 'NORMAL', 'NONE'
            
        # 밝기는 적절하지 않지만 다른 문제는 없는 경우
        return 'DIM_OVERALL', 'LOW'

    def _extract_technical_details(self, analysis: Dict) -> Dict[str, Any]:
        """기술적 세부사항 추출"""
        return {
            'mean_brightness': analysis['mean_brightness'],
            'brightness_range': analysis['brightness_range'],
            'cv_score': analysis['cv_analysis']['cv'],
            'cv_grade': analysis['cv_analysis']['grade'],
            'percentile_ratio': analysis['percentile_analysis']['p90_p10_ratio'],
            'spatial_score': analysis['spatial_analysis']['spatial_uniformity_score'],
            'weighted_score': analysis['weighted_score'],
            'issues': analysis['technical_summary']
        }

    def get_diagnosis_korean(self, diagnosis: str) -> str:
        """진단명 한글 변환"""
        return self.diagnosis_mapping.get(diagnosis, '알 수 없음')

    def get_severity_level(self, diagnosis: str) -> str:
        """심각도 수준 반환"""
        severity_map = {
            'NORMAL': 'NONE',
            'DIM_OVERALL': 'MEDIUM',
            'DIM_PARTIAL': 'MEDIUM', 
            'UNEVEN_BRIGHTNESS': 'LOW',
            'HOTSPOT': 'MEDIUM',
            'FLICKERING': 'HIGH',
            'OFF': 'HIGH',
            'UNKNOWN': 'HIGH'
        }
        return severity_map.get(diagnosis, 'MEDIUM')

    def generate_recommendation(self, diagnosis: str) -> str:
        """진단에 따른 권장사항 생성"""
        recommendations = {
            'NORMAL': '정상 상태입니다. 정기 점검을 계속하세요.',
            'DIM_OVERALL': '램프 전체가 어둡습니다. 전구 교체나 전력 공급을 확인하세요.',
            'DIM_PARTIAL': '램프 일부가 어둡습니다. 내부 반사판이나 렌즈를 점검하세요.',
            'UNEVEN_BRIGHTNESS': '밝기가 불균일합니다. 렌즈 청소나 반사판 점검이 필요합니다.',
            'HOTSPOT': '과도하게 밝은 부분이 있습니다. 렌즈나 반사판 손상을 확인하세요.',
            'FLICKERING': '깜빡임이 감지됩니다. 전기 연결부나 전구를 점검하세요.',
            'OFF': '램프가 꺼져 있습니다. 전구나 전력 공급을 즉시 확인하세요.',
            'UNKNOWN': '판단할 수 없습니다. 수동 점검이 필요합니다.'
        }
        return recommendations.get(diagnosis, '전문가의 점검이 필요합니다.')
