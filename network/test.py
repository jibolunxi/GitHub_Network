class Solution:
    #利用字典实现交集
    def intersect(self,nums1,nums2):
        record={}
        result=[]
        for i in nums1:
            # 注意python3.x用contains()方法取代has_key()
            if(record.__contains__(i)):
                record[i]+=1                      #将字典的key值数+1
            else:
                record[i]=1                       #将i加入字典中 key为1
        for j in nums2:
            if(record.__contains__(j) and record[j]>0):
                record[j]-=1                      #i的key值减1
                result.append(j)
        return result
if __name__=="__main__":
    s=Solution()
    num1=[1,2,3,4,4,4,1]
    num2=[4,4,2,3]
    print(s.intersect(num1,num2))