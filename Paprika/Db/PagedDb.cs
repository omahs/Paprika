using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Paprika.Db;

public abstract unsafe class PagedDb : IDb, IDisposable
{
    /// <summary>
    /// The number of roots kept in the history.
    /// At least two are required to make sure that the writing transaction does not overwrite the current root.
    /// </summary>
    /// <remarks>
    /// REORGS
    /// It can be set arbitrary big and used for handling reorganizations.
    /// If history depth is set to the max reorg depth,
    /// moving to previous block is just a single write transaction moving the root back.
    ///
    /// ABANDONED PAGES
    /// To keep N roots active, the pages that were abandoned in previous transactions should be reused only on
    /// rolling over, meaning, that they should be taken from the item that the root will point to. In this case,
    /// if undo happens, they are still active. So use abandoned pages and add them to abandoned pages of the given
    /// transaction, use at will and commit them so that they can be reused in <see cref="HistoryDepth"/> commits. 
    /// </remarks>
    private const int HistoryDepth = 2;

    private readonly int _maxPage;
    private readonly MetadataPage[] _metadata;

    private long _currentRoot;

    protected PagedDb(ulong size)
    {
        _maxPage = (int)(size / Page.Size);
        _metadata = new MetadataPage[HistoryDepth];
    }

    protected void RootInit()
    {
        for (var i = 0; i < HistoryDepth; i++)
        {
            _metadata[i] = GetAt(i).As<MetadataPage>();
        }

        if (_metadata[0].Data.NextFreePage < HistoryDepth)
        {
            // the 0th page will have the properly number set to first free page
            _metadata[0].Data.NextFreePage = HistoryDepth;
        }

        _currentRoot = 0;
        for (var i = 0; i < HistoryDepth; i++)
        {
            if (_metadata[i].Header.TxId > _currentRoot)
            {
                _currentRoot = _metadata[i].Header.TxId;
            }
        }
    }

    protected abstract void* Ptr { get; }

    public double TotalUsedPages => (double)CurrentMeta.Data.NextFreePage / _maxPage;

    private ref readonly MetadataPage CurrentMeta => ref _metadata[_currentRoot % HistoryDepth];
    private ref MetadataPage NextMeta => ref _metadata[(_currentRoot + 1)% HistoryDepth];

    private void MoveRootNext() => _currentRoot++;

    private Page GetAt(int address)
    {
        if (address > _maxPage)
            throw new ArgumentException($"Requested address {address} while the max page is {_maxPage}");

        // Long here is required! Otherwise int overflow will turn it to negative value!
        var offset = ((long)address) * Page.Size;
        return new Page((byte*)Ptr + offset);
    }

    private int GetAddress(in Page page)
    {
        return (int)(Unsafe.ByteOffset(ref Unsafe.AsRef<byte>(Ptr), ref Unsafe.AsRef<byte>(page.Raw.ToPointer()))
            .ToInt64() / Page.Size);
    }

    public abstract void Dispose();
    protected abstract void Flush();

    public ITransaction Begin() => new Transaction(this);

    class Transaction : ITransaction, IInternalTransaction
    {
        private readonly PagedDb _db;
        private readonly MetadataPage _meta;
        private Page _root;

        public Transaction(PagedDb db)
        {
            _db = db;

            // set next id
            TxId = _db.CurrentMeta.Header.TxId++;
            
            // copy to next meta
            _db.NextMeta = _db.CurrentMeta;
            _meta = _db.NextMeta;

            // peek the next free and treat it as root
            var newRoot = _meta.GetNextFreePage();
            _root = _db.GetAt(newRoot);
            _meta.Data.Root = newRoot;
            _root.Clear();
            
            // mark as the current
            _root.Header.TxId = TxId;

            // copy current
            var prevRoot = _db.CurrentMeta.Data.Root;
            if (prevRoot != 0)
            {
                _db.GetAt(prevRoot).CopyTo(_root);

                // abandon the previous root that has been copied to the new one
                _meta.Abandon(prevRoot);
            }
        }

        public bool TryGet(in ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
        {
            var path = NibblePath.FromKey(key);
            return _root.TryGet(path, 0, this, out value);
        }

        public void Set(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
        {
            var path = NibblePath.FromKey(key);
            _root = _root.Set(path, value, 0, this);
        }

        public void Commit()
        {
            // flush data first
            _meta.Data.Root = _db.GetAddress(_root);

            _db.Flush();

            // flush tx id so that it's persistent
            _meta.Header.TxId = TxId;
            _db.MoveRootNext();
            
            _db.Flush();
        }

        public double TotalUsedPages => (double)_meta.Data.NextFreePage / _db._maxPage;

        public long TxId { get; }

        Page IInternalTransaction.GetAt(int address) => _db.GetAt(address);

        int IInternalTransaction.GetAddress(in Page page) => _db.GetAddress(page);

        Page IInternalTransaction.GetNewDirtyPage(out int addr)
        {
            addr = _meta.GetNextFreePage();

            if (addr >= _db._maxPage)
                throw new Exception("The db file is too small for this page");

            return _db.GetAt(addr);
        }

        void IInternalTransaction.Abandon(in Page page) => _meta.Abandon(_db.GetAddress(page));
    }
}