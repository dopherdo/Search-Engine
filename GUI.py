import tkinter as tk
from tkinter import ttk, scrolledtext
from search_engine_main import run_query

class SearchEngineGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Search Engine")
        self.root.geometry("600x400")
        
        # Set theme and colors
        self.style = ttk.Style()
        self.style.configure('Custom.TFrame', background='#f0f2f5')
        self.style.configure('Search.TButton', 
                           padding=5, 
                           font=('Helvetica', 10, 'bold'))
        
        # Create main frame with custom background
        main_frame = ttk.Frame(root, padding="20", style='Custom.TFrame')
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Title label
        title_label = ttk.Label(main_frame, 
                              text="Web Search", 
                              font=('Helvetica', 16, 'bold'),
                              background='#f0f2f5')
        title_label.grid(row=0, column=0, pady=(0, 20))
        
        # Search frame
        search_frame = ttk.Frame(main_frame, style='Custom.TFrame')
        search_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        search_frame.columnconfigure(0, weight=1)
        
        # Custom search entry with placeholder
        self.search_var = tk.StringVar()
        self.search_entry = ttk.Entry(search_frame, 
                                    textvariable=self.search_var,
                                    font=('Helvetica', 11))
        self.search_entry.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 5))
        
        # Add placeholder text
        self.placeholder = "Enter your search query here..."
        self.search_entry.insert(0, self.placeholder)
        self.search_entry.config(foreground='gray')
        
        # Bind focus events for placeholder behavior
        self.search_entry.bind('<FocusIn>', self.on_entry_click)
        self.search_entry.bind('<FocusOut>', self.on_focus_out)
        
        # Search button with custom style
        self.search_button = ttk.Button(search_frame, 
                                      text="Search", 
                                      command=self.perform_search,
                                      style='Search.TButton')
        self.search_button.grid(row=0, column=1)
        
        # Clear button
        self.clear_button = ttk.Button(search_frame,
                                     text="Clear",
                                     command=self.clear_search,
                                     style='Search.TButton')
        self.clear_button.grid(row=0, column=2, padx=(5, 0))
        
        # Results label
        self.results_label = ttk.Label(main_frame, 
                                     text="Search Results", 
                                     font=('Helvetica', 11, 'bold'),
                                     background='#f0f2f5')
        self.results_label.grid(row=2, column=0, sticky=tk.W, pady=(10, 5))
        
        # Results area with custom styling - now with read-only state
        self.results_area = scrolledtext.ScrolledText(
            main_frame,
            wrap=tk.WORD,
            height=15,
            font=('Helvetica', 10),
            background='#ffffff',
            borderwidth=1,
            relief="solid",
            state='disabled'  # Make it read-only
        )
        self.results_area.grid(row=3, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Change cursor to arrow for results area to indicate it's not editable
        self.results_area.config(cursor="arrow")
        
        # Status bar
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        self.status_bar = ttk.Label(main_frame, 
                                  textvariable=self.status_var,
                                  background='#f0f2f5',
                                  font=('Helvetica', 9))
        self.status_bar.grid(row=4, column=0, sticky=(tk.W, tk.E), pady=(5, 0))
        
        # Configure grid weights
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(3, weight=1)
        root.columnconfigure(0, weight=1)
        root.rowconfigure(0, weight=1)
        
        # Bind Enter key to search
        self.search_entry.bind('<Return>', lambda e: self.perform_search())
        
    def on_entry_click(self, event):
        """Handle entry field click - clear placeholder text"""
        if self.search_var.get() == self.placeholder:
            self.search_entry.delete(0, tk.END)
            self.search_entry.config(foreground='black')

    def on_focus_out(self, event):
        """Handle focus out - restore placeholder if empty"""
        if not self.search_var.get():
            self.search_entry.insert(0, self.placeholder)
            self.search_entry.config(foreground='gray')

    def clear_search(self):
        """Clear search field and results"""
        self.search_var.set("")
        self.results_area.config(state='normal')  # Temporarily enable to clear
        self.results_area.delete(1.0, tk.END)
        self.results_area.config(state='disabled')  # Make read-only again
        self.status_var.set("Ready")
        self.on_focus_out(None)

    def perform_search(self):
        """Handle search operation"""
        query = self.search_var.get()
        if query and query != self.placeholder:
            self.status_var.set("Searching...")
            self.root.update()
            
            # Enable results area temporarily for updating
            self.results_area.config(state='normal')
            
            # Clear previous results
            self.results_area.delete(1.0, tk.END)
            
            # Add new results
            self.results_area.insert(tk.END, f"Search results for: {query}\n\n")
            self.results_area.tag_configure("link", foreground="blue", underline=1)

            # Call run_query and get the results
            results, elapsed_time = run_query(query)

            # Print out results in order (1-5)
            for idx, result in enumerate(results[:5], start=1):  # Limit to top 5 results
                self.results_area.insert(tk.END, f"{idx}. ➜ {result}\n", "link")
            
            # Make results area read-only again
            self.results_area.config(state='disabled')
            
            self.status_var.set(f"Found top {len(results)} results in {elapsed_time:02f} seconds")

def main():
    root = tk.Tk()
    app = SearchEngineGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()