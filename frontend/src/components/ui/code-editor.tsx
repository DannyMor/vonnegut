import { useRef, useEffect } from "react";
import { EditorView, keymap, placeholder as cmPlaceholder } from "@codemirror/view";
import { EditorState, type Extension } from "@codemirror/state";
import { defaultKeymap, indentWithTab } from "@codemirror/commands";
import { lineNumbers, highlightActiveLineGutter, highlightActiveLine } from "@codemirror/view";
import { bracketMatching, indentOnInput } from "@codemirror/language";
import { oneDark } from "@codemirror/theme-one-dark";
import { python } from "@codemirror/lang-python";
import { sql, PostgreSQL, MySQL, StandardSQL } from "@codemirror/lang-sql";
import type { ConnectionConfig } from "@/types/connection";

/** Map connection config type to a CodeMirror SQL dialect. Add new entries as connection types grow. */
const CONNECTION_TYPE_DIALECTS: Record<string, typeof PostgreSQL> = {
  postgres_direct: PostgreSQL,
  postgres_pod: PostgreSQL,
  mysql_direct: MySQL,
  singlestore_direct: MySQL,
  snowflake_direct: StandardSQL,
};

/** Get the appropriate CodeMirror language extension for a connection type. */
export function languageForConnection(config?: ConnectionConfig | null): Extension {
  const dialect = config ? CONNECTION_TYPE_DIALECTS[config.type] : undefined;
  return sql({ dialect: dialect ?? PostgreSQL });
}

export type EditorLanguage = "python" | "sql" | Extension;

interface Props {
  value: string;
  onChange: (value: string) => void;
  language: EditorLanguage;
  placeholder?: string;
  className?: string;
  /** When true, the editor stretches to fill its parent via flex. */
  flexGrow?: boolean;
}

export function CodeEditor({ value, onChange, language, placeholder, className, flexGrow }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const onChangeRef = useRef(onChange);
  onChangeRef.current = onChange;

  // Stable key for recreating editor when language identity changes
  const langKey = typeof language === "string" ? language : "custom";

  useEffect(() => {
    if (!containerRef.current) return;

    let langExtension: Extension;
    if (language === "python") langExtension = python();
    else if (language === "sql") langExtension = sql();
    else langExtension = language;

    const state = EditorState.create({
      doc: value,
      extensions: [
        lineNumbers(),
        highlightActiveLineGutter(),
        highlightActiveLine(),
        bracketMatching(),
        indentOnInput(),
        langExtension,
        oneDark,
        keymap.of([...defaultKeymap, indentWithTab]),
        EditorView.updateListener.of((update) => {
          if (update.docChanged) {
            onChangeRef.current(update.state.doc.toString());
          }
        }),
        EditorView.theme({
          "&": {
            fontSize: "13px",
            borderRadius: "6px",
            border: "1px solid var(--border)",
            ...(flexGrow ? { height: "100%" } : {}),
          },
          ".cm-scroller": { overflow: "auto" },
          ".cm-content": { ...(!flexGrow ? { minHeight: "200px" } : {}), padding: "8px 0" },
          ".cm-gutters": { borderRadius: "6px 0 0 6px" },
        }),
        ...(placeholder ? [cmPlaceholder(placeholder)] : []),
      ],
    });

    const view = new EditorView({ state, parent: containerRef.current });
    viewRef.current = view;

    return () => {
      view.destroy();
      viewRef.current = null;
    };
    // Only recreate on language change, not on value changes
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [langKey, flexGrow]);

  // Sync external value changes without recreating the editor
  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;
    const current = view.state.doc.toString();
    if (current !== value) {
      view.dispatch({
        changes: { from: 0, to: current.length, insert: value },
      });
    }
  }, [value]);

  return (
    <div
      ref={containerRef}
      className={className}
      style={flexGrow ? { flex: 1, minHeight: 0 } : undefined}
    />
  );
}
