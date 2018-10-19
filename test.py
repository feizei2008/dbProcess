import os

class Foo():
    def bar(self):
        print "bar"

    def hello(self, name):
        print "I'm %s" % name

    def createtxt(self):
        file = open(os.getcwd() + "\\" + "b.txt",'w')
        file.write('hello,Zzack')
        file.close()
obj = Foo()
obj.createtxt()
# obj.bar()
# obj.hello('zack')

file = open(os.getcwd() + "\\" + "a.txt",'w')
file.write('hellozack')
file.close()

# def foo(bar=[]):
#     if not bar:
#         bar = []
#         bar.append('hello')
#     return bar
#
# print foo()
# print foo()