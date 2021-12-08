import os,threading
import pandas as pd


# 这个脚本中我把makeblastdb和tblastn放在一起处理了，实际上还是分开进行会比较有效率一点。
# 此处是将十几个酶一起进行blast，即首先进行建库(makeblastdb)，再以生成的库用每个酶进行比对。
# 将每个数据库比对生成的结果放在一个文件夹中读取结果文件的内容
# 生成了一个数据框，可以查看某个物种中找到的同源序列的数量

datas = os.listdir("data/")
f1 = datas[0:2]
f2 = datas[2:5]
f3 = datas[5:7]
f4 = datas[7:9]
files = os.listdir("enzyme/")

df = pd.DataFrame()

def main():
    run6()

def tblastn_fmt6(fp):
    print(f1)
    for genome in fp:
        print(genome)
        jieya = "gzip -d data/" + genome
        print(jieya)
        os.system(jieya)
        instr = "makeblastdb -in data/" + genome.strip(".fna.gz") + ".fna" + " -dbtype nucl " + "-out db/" + genome.strip('.fna.gz') + "/" + genome.strip(".fna.gz")
        os.system(instr)
        for enzyme in files:
            old_name = "./enzyme/" + enzyme
            new_name = "./enzyme/" + enzyme.replace("(","_").replace(")","")
            # print(new_name)
            os.rename(old_name,new_name)
            os.system("mkdir output")
            os.system("mkdir output/" + genome.strip(".fna.gz"))
            blast = "tblastn -query enzyme/" + new_name.strip("./enzyme/") + ' -evalue 0.01 -db db/' + genome.strip('.fna.gz') + "/" + genome.strip(".fna.gz") + " -out output/" + genome.strip(".fna.gz") + "/" + enzyme + ".blast" + " -outfmt 6"
            # print(blast)
            os.system(blast)
        blasto = os.listdir("./output/" + genome.strip(".fna.gz") + "/")
        list = []
        list1 = []
        for result in blasto:
            ops = "output/" + genome.strip(".fna.gz") + "/" + result
            # print(ops)
            with open (ops,"r") as op:
                lines = op.readlines()
                list.append(result.strip(".fasta.blast"))
                list1.append(len(lines))
        dic = dict(map(lambda x,y:[x,y],list,list1)) # 生成了酶名和找到的同原序列个数的字典 

        new = pd.DataFrame(dic,index=[genome])      # 将字典转换为dataframe，index设为基因组名

        global df
        df = df.append(new)   # 添加到最终的数据框

def run6():   # 基本线程操作

    t1=threading.Thread(target=tblastn_fmt6,args=(f1,))
    t2=threading.Thread(target=tblastn_fmt6,args=(f2,))
    t3=threading.Thread(target=tblastn_fmt6,args=(f3,))
    t4=threading.Thread(target=tblastn_fmt6,args=(f4,))

    t1.start()
    t2.start()
    t3.start()
    t4.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()
 

    print(df)

if __name__=='__main__':
    main()