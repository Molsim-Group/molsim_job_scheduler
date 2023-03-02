import sys
from datetime import datetime
import numpy as np
#import random

lunch_list = ["잇마이타이", "궁칼국수", "삼부자 부대찌개", "엉클 부대찌개", "하레", "동해", "국수나무", '111-7', 
              '맘스터치', '어랑족', '더큰도시락', '오마이동', '요시다', '세번째 우물', 
              '버기즈', '쌈의 대가', '콩사랑 굴내음', '샌드브런치', '서브웨이', '배달', '안골 칼국수', '본죽', '어은 국수', 
              '화로에 굽다', '맑음', '한마음 정육식당', '고봉민 김밥', '형제 돌구이', '맛고을']

today = datetime.today()
seed = int(f'{today.year}{today.month}{today.day}')

lunch_list = np.array(lunch_list, dtype=object)

argv = sys.argv

try:
    num = int(argv[1])
except IndexError:
    num = 3

np.random.seed(seed)
selection = np.random.choice(lunch_list, num, replace=False)

for i, key in enumerate(selection, 1):
    print (f"{i} 순위 : {key}")
