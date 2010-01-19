
class Foo:
    cat = "meow"

    def make_bar(self):
        class Bar:
            dog = "bark"
            cat = self.cat
        return Bar

foo = Foo()
Bar = foo.make_bar()
bar = Bar()
print bar.cat
