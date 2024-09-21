using System;
using System.Collections.Generic;

public class BTreeNode
{
    public List<int> Keys; // Claves almacenadas en el nodo
    public List<BTreeNode> Children; // Hijos del nodo
    public bool IsLeaf; // Es hoja o no

    public BTreeNode(bool isLeaf)
    {
        IsLeaf = isLeaf;
        Keys = new List<int>();
        Children = new List<BTreeNode>();
    }
}

public class BTree
{
    private BTreeNode root;
    private int degree; // Grado mínimo (t) del árbol B

    public BTree(int degree)
    {
        this.degree = degree;
        root = new BTreeNode(true); // Crear un nodo raíz vacío que es una hoja
    }

    // Función para insertar una nueva clave en el árbol B
    public void Insert(int key)
    {
        BTreeNode r = root;

        // Si la raíz está llena, el árbol debe crecer en altura
        if (r.Keys.Count == 2 * degree - 1)
        {
            BTreeNode s = new BTreeNode(false); // Nuevo nodo raíz
            s.Children.Add(r); // Hacer que la raíz actual sea hija del nuevo nodo
            SplitChild(s, 0, r); // Dividir el nodo hijo
            root = s; // Actualizar la raíz

            InsertNonFull(s, key); // Insertar la clave en el nodo adecuado
        }
        else
        {
            InsertNonFull(r, key);
        }
    }

    // Función para dividir un nodo hijo cuando está lleno
    private void SplitChild(BTreeNode parent, int index, BTreeNode fullChild)
    {
        BTreeNode newChild = new BTreeNode(fullChild.IsLeaf);
        parent.Children.Insert(index + 1, newChild);
        parent.Keys.Insert(index, fullChild.Keys[degree - 1]);

        // Mover la mitad superior de las claves y los hijos al nuevo nodo
        for (int i = 0; i < degree - 1; i++)
        {
            newChild.Keys.Add(fullChild.Keys[degree + i]);
        }
        fullChild.Keys.RemoveRange(degree - 1, degree);

        if (!fullChild.IsLeaf)
        {
            for (int i = 0; i < degree; i++)
            {
                newChild.Children.Add(fullChild.Children[degree + i]);
            }
            fullChild.Children.RemoveRange(degree, degree);
        }
    }

    // Función para insertar en un nodo que no está lleno
    private void InsertNonFull(BTreeNode node, int key)
    {
        int i = node.Keys.Count - 1;

        if (node.IsLeaf)
        {
            // Insertar la clave en la posición correcta en un nodo hoja
            node.Keys.Add(0); // Añadir espacio para la nueva clave
            while (i >= 0 && key < node.Keys[i])
            {
                node.Keys[i + 1] = node.Keys[i];
                i--;
            }
            node.Keys[i + 1] = key;
        }
        else
        {
            // Encontrar el hijo correcto para descender
            while (i >= 0 && key < node.Keys[i])
            {
                i--;
            }
            i++;

            // Si el hijo está lleno, dividirlo
            if (node.Children[i].Keys.Count == 2 * degree - 1)
            {
                SplitChild(node, i, node.Children[i]);

                // Después de la división, decidir qué hijo seguir
                if (key > node.Keys[i])
                {
                    i++;
                }
            }
            InsertNonFull(node.Children[i], key);
        }
    }

    // Función para buscar una clave en el árbol B
    public bool Search(int key)
    {
        return SearchInternal(root, key);
    }

    private bool SearchInternal(BTreeNode node, int key)
    {
        int i = 0;
        while (i < node.Keys.Count && key > node.Keys[i])
        {
            i++;
        }

        if (i < node.Keys.Count && key == node.Keys[i])
        {
            return true; // Clave encontrada
        }

        if (node.IsLeaf)
        {
            return false; // Si es hoja, no se encontró la clave
        }

        return SearchInternal(node.Children[i], key); // Buscar en el subárbol
    }
}
