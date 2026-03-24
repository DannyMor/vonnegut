import { useState, useMemo, useRef, useCallback, useEffect } from "react";
import { Search, Loader2 } from "lucide-react";
import { Input } from "@/components/ui/input";
import { fuzzyMatch } from "@/lib/utils";

export interface SelectOption {
  label: string;
  value: string;
}

interface Props {
  /** Simple string items — label and value are the same */
  items?: string[];
  /** Label/value pairs — use when value differs from display text */
  options?: SelectOption[];
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  disabled?: boolean;
  loading?: boolean;
  className?: string;
}

export function SearchableSelect({ items, options: optionsProp, value, onChange, placeholder = "Search...", disabled, loading, className }: Props) {
  const options: SelectOption[] = useMemo(() => {
    if (optionsProp) return optionsProp;
    return (items ?? []).map((item) => ({ label: item, value: item }));
  }, [items, optionsProp]);

  // Resolve current label from value
  const valueLabel = useMemo(() => options.find((o) => o.value === value)?.label ?? value, [options, value]);

  const [filter, setFilter] = useState(valueLabel);
  const [open, setOpen] = useState(false);
  const [highlightIndex, setHighlightIndex] = useState(0);
  const listRef = useRef<HTMLDivElement>(null);

  const filtered = useMemo(() => {
    if (!filter) return options;
    return options.filter((o) => fuzzyMatch(o.label, filter));
  }, [options, filter]);

  // Sync external value changes
  useEffect(() => {
    const label = options.find((o) => o.value === value)?.label ?? value;
    setFilter(label);
  }, [value, options]);

  // Reset highlight when filter changes
  useEffect(() => { setHighlightIndex(0); }, [filtered.length]);

  // Scroll highlighted item into view
  useEffect(() => {
    if (!open || !listRef.current) return;
    const el = listRef.current.children[highlightIndex] as HTMLElement | undefined;
    el?.scrollIntoView({ block: "nearest" });
  }, [highlightIndex, open]);

  const select = useCallback((opt: SelectOption) => {
    setFilter(opt.label);
    onChange(opt.value);
    setOpen(false);
  }, [onChange]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (!open) {
      if (e.key === "ArrowDown" || e.key === "Enter") {
        e.preventDefault();
        setOpen(true);
      }
      return;
    }
    switch (e.key) {
      case "ArrowDown":
        e.preventDefault();
        setHighlightIndex((i) => Math.min(i + 1, filtered.length - 1));
        break;
      case "ArrowUp":
        e.preventDefault();
        setHighlightIndex((i) => Math.max(i - 1, 0));
        break;
      case "Enter":
        e.preventDefault();
        if (filtered[highlightIndex]) select(filtered[highlightIndex]);
        break;
      case "Escape":
        e.preventDefault();
        setOpen(false);
        break;
    }
  };

  return (
    <div className={`relative ${className ?? ""}`}>
      <div className="relative">
        {loading ? (
          <Loader2 className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground pointer-events-none animate-spin" />
        ) : (
          <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground pointer-events-none" />
        )}
        <Input
          className="pl-8"
          value={filter}
          placeholder={loading ? "Loading..." : placeholder}
          disabled={disabled || loading}
          onChange={(e) => { setFilter(e.target.value); setOpen(true); }}
          onFocus={() => { setOpen(true); setFilter(""); }}
          onBlur={() => setTimeout(() => {
            setOpen(false);
            // Restore label if nothing was selected
            const label = options.find((o) => o.value === value)?.label ?? value;
            setFilter(label);
          }, 150)}
          onKeyDown={handleKeyDown}
        />
      </div>
      {open && filtered.length > 0 && (
        <div
          ref={listRef}
          className="absolute z-50 mt-1 w-full border rounded bg-popover text-popover-foreground shadow-md max-h-48 overflow-y-auto"
        >
          {filtered.map((opt, i) => (
            <button
              key={opt.value}
              type="button"
              className={`w-full text-left px-3 py-1.5 text-sm truncate ${
                i === highlightIndex ? "bg-muted" : ""
              } ${opt.value === value ? "font-medium" : ""}`}
              onMouseDown={(e) => { e.preventDefault(); select(opt); }}
              onMouseEnter={() => setHighlightIndex(i)}
            >
              {opt.label}
            </button>
          ))}
        </div>
      )}
      {open && filter && filtered.length === 0 && (
        <div className="absolute z-50 mt-1 w-full border rounded bg-popover text-popover-foreground shadow-md px-3 py-2 text-xs text-muted-foreground">
          No matches
        </div>
      )}
    </div>
  );
}
