from reactors import Reactor


def main():
    """Main function"""
    r = Reactor()
    r.logger("Hello this is actor {}".format(r.uid))


if __name__ == '__main__':
    main()
