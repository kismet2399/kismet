import jieba
from sklearn.feature_extraction.text import CountVectorizer
import token

# dict_file = 'user_dict.txt'
# jieba.load_userdict(dict_file)
s = "中文分词和大数据还有云计算"
# s='中国好声音'

print('/'.join(jieba.cut(s)))

s_list = ['中文分词中文计算中文分词','大数据中国好声音中文分词','云计算中国好声音','用结巴分词来做中文分词','云计算大数据']
s_l = [' '.join(jieba.cut(x)) for x in s_list]
print(s_l)
# 新词发现
#  ngram_range : tuple (min_n, max_n)
#        The lower and upper boundary of the range of n-values for different
#        n-grams to be extracted. All values of n such that min_n <= n <= max_n
#        will be used. 【中文，分词】，中文，计算
vectorizer = CountVectorizer(ngram_range=(2, 3),token_pattern=r"\b\w+\b", min_df=0.3)
x1 = vectorizer.fit_transform(s_l)
print(x1)
print(vectorizer.vocabulary_)
print([x.replace(' ','') for x in vectorizer.vocabulary_.keys()])
