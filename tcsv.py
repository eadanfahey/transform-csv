import csv

class ConstraintError(Exception):

    def __init__(self, column, value, fn_name, rownumber):
        self.column = column
        self.value = value
        self.fn_name = fn_name
        self.rown = rownumber

    def __str__(self):
        message = "{} value {} does not satisfy the constraint {} on row {}"
        return message.format(self.column, self.value, self.fn_name, self.rown)


class TransformError(Exception):

    def __init__(self, row, err):
        self.row = row
        self.err = err

    def __str__(self):
        return "on csv row {} with error: {}".format(self.row, self.err)


class TransformCSV(object):

    def __init__(self, input_file, skiprows=0):
        self.rownumber = 1
        self.ifile = open(input_file)
        self.names = None
        self.reader = None
        self.idx = None
        self._create_reader(skiprows)
        self.mutation_fns = []
        self.constraint_fns = []
        self.select_fn = lambda row: row

    def __iter__(self):
        return self

    def __next__(self):
        row = next(self.reader)
        self.rownumber += 1
        try:
            mutated = row[:]
            for fn in self.mutation_fns:
                mutated = fn(row)
            mutated_cp = mutated[:]
            for cfn in self.constraint_fns:
                cfn(mutated_cp)
            return self.select_fn(mutated)
        except Exception as e:
            raise TransformError(self.rownumber, e)

    def close(self):
        self.ifile.close()

    def _create_reader(self, skiprows):
        """
        Create a csv reader object from the input csv file.
        """
        # with open(self.input_file) as f:
        reader = csv.reader(self.ifile)
        for _ in range(skiprows):
            next(reader)
            self.rownumber += 1
        names = next(reader)
        self.reader = reader
        self.names = names
        self.idx = dict(zip(names, range(len(names))))

    def rename(self, name_map):
        """
        Change the column names.
        Args:
            name_map: A dictionary mapping the current names to new names.
        Returns:
            None
        """
        new_names = []
        for name in self.names:
            new_name = name_map.get(name)
            if new_name is None:
                new_names.append(name)
            else:
                new_names.append(new_name)
        self.names = new_names
        self.idx = dict(zip(self.names, range(len(self.names))))

    def add(self, name, val):
        """
        Add a column to the csv containing a constant value.
        TODO: replace this method with add_column
        Args:
            name: The name of the new column.
            val: The value to place in each row of the new column.
        Returns:
            None
        """
        def f(row):
            row.append(val)
            return row

        self.mutation_fns.append(f)
        self.names.append(name)
        self.idx[name] = len(self.names) - 1

    def add_column(self, name, fn, col):
        """
        Add a column to the csv with the new value produced by a
        user defined function that can access all entries on the same row.
        TODO: Perhaps I use inspect.signature to verify that the number of
        arguments that `fn` takes is the same as the number of columns
        passed. But, this doesn't work for some builting functions e.g. int.
        Args:
            name: The name of the new column.
            fn: The function to apply to the row.
            col: The columns that are arguments to the function.
        Returns:
            None
        """
        if isinstance(col, str):
            columns = [col]
        elif isinstance(col, list) or isinstance(col, tuple):
            columns = col
        else:
            raise TypeError('The parameter col must be of type str, list or tuple')

        # check that the column names are valid.
        for c in columns:
            try:
                self.idx[c]
            except KeyError:
                raise KeyError("The column '{}' does not exist".format(c))

        def add_column_fn(row):
            vals = [row[self.idx[c]] for c in columns]
            new_val = fn(*vals)
            row.append(new_val)
            return row

        self.mutation_fns.append(add_column_fn)
        self.names.append(name)
        self.idx[name] = len(self.names) - 1


    def mutate(self, fn, col=None):
        """
        Mutate a column by applying a function to it.
        Args:
            fn: The function to apply. Takes a string or numeric argument and
                returns a string or numeric argument.
            col: The name of the column to be mutated. Can be of three forms:
                1) None (default): the function is applied to all columns.
                2) list/tuple of column names to apply the function to.
                3) A single column name to apply the function to.
        Returns:
            None
        Raises:
            TypeError: The parameter `col` is the wrong type.
            KeyError: When trying to mutate a column that doesn't exist.
        """
        if col is None:
            columns = self.names
        elif isinstance(col, str):
            columns = [col]
        elif isinstance(col, list) or isinstance(col, tuple):
            columns = col
        else:
            raise TypeError("col must be of type None, str, list or tuple")

        # check that the column names are valid.
        for c in columns:
            try:
                self.idx[c]
            except KeyError:
                raise KeyError("The column '{}' does not exist".format(c))

        def mutate_fn(row):
            for c in columns:
                row[self.idx[c]] = fn(row[self.idx[c]])
            return row

        self.mutation_fns.append(mutate_fn)

    def constraint(self, fn, col):
        """
        Check that a column satisfies a constraint.
        Args:
            fn: A function of a single argument that returns True if the
                column value satisfies the constraint, or False otherwise.
            col: The name of the column to check.
        Returns:
            None
        Raises:
            ConstraintError: If fn returns False.
            TypeError: If col is not the correct type.
            KeyError: If a column name does not exist.
        """
        if col is None:
            columns = self.names
        elif isinstance(col, str):
            columns = [col]
        elif isinstance(col, list) or isinstance(col, tuple):
            columns = col
        else:
            raise TypeError("col must be of type None, str, list or tuple")

        # check that the column names are valid.
        for c in columns:
            try:
                self.idx[c]
            except KeyError:
                raise KeyError("The column '{}' does not exist".format(c))

        def constraint_fn(row):
            for c in columns:
                val = row[self.idx[c]]
                if not fn(val):
                    raise ConstraintError(c, val, fn.__name__, self.rownumber)
        self.constraint_fns.append(constraint_fn)

    def select(self, columns):
        """
        Select only the supplied columns.
        columns:
            columns: A list of column names to select.
        Returns:
            None
        Raises:
            KeyError: If a column does not exist.
        """
        for c in columns:
            try:
                self.idx[c]
            except KeyError:
                raise KeyError("The column '{}' does not exist".format(c))

        def select_fn(row):
            return [row[self.idx[col]] for col in columns]

        self.names = columns
        self.select_fn = select_fn


    def write(self, filename):
        """
        Write the csv to file. This will exhaust the iterator.
        Args:
            filename: the name of the csv file.
        Returns:
            None
        Raises:
            FileNotFoundError: the file could not be created.
        """
        with open(filename, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(self.names)
            while True:
                try:
                    writer.writerow(self.__next__())
                except StopIteration:
                    break

