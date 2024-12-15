from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton, QFileDialog, QTextEdit, QInputDialog, QListWidget, QListWidgetItem, QHBoxLayout, QComboBox
from PyQt5.QtCore import QItemSelectionModel  # Correct import statement
import sys
import re

class M3UEditor(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()
        self.categories = {}

    def initUI(self):
        self.setWindowTitle('M3U Editor')

        layout = QVBoxLayout()

        self.loadButton = QPushButton('Load M3U')
        self.saveButton = QPushButton('Save M3U')
        self.newButton = QPushButton('New M3U')
        self.addCategoryButton = QPushButton('Add Category')
        self.addChannelButton = QPushButton('Add Channel')
        self.categoriesButton = QPushButton('Categories')
        self.mergeButton = QPushButton('Merge M3Us')

        self.loadButton.clicked.connect(self.loadM3U)
        self.saveButton.clicked.connect(self.saveM3U)
        self.newButton.clicked.connect(self.newM3U)
        self.addCategoryButton.clicked.connect(self.addCategory)
        self.addChannelButton.clicked.connect(self.addChannel)
        self.categoriesButton.clicked.connect(self.showCategories)
        self.mergeButton.clicked.connect(self.mergeM3Us)

        self.textEdit = QTextEdit()

        layout.addWidget(self.loadButton)
        layout.addWidget(self.saveButton)
        layout.addWidget(self.newButton)
        layout.addWidget(self.addCategoryButton)
        layout.addWidget(self.addChannelButton)
        layout.addWidget(self.categoriesButton)
        layout.addWidget(self.mergeButton)
        layout.addWidget(self.textEdit)

        self.setLayout(layout)

    def loadM3U(self):
        options = QFileDialog.Options()
        fileName, _ = QFileDialog.getOpenFileName(self, "Open M3U File", "", "M3U Files (*.m3u);;All Files (*)", options=options)
        if fileName:
            try:
                with open(fileName, 'r', encoding='utf-8') as file:
                    content = file.read()
            except UnicodeDecodeError:
                with open(fileName, 'r', encoding='latin-1') as file:
                    content = file.read()
            self.categories = parse_m3u(content)
            self.updateTextEdit()
            self.textEdit.setPlainText(content)

    def saveM3U(self):
        options = QFileDialog.Options()
        fileName, _ = QFileDialog.getSaveFileName(self, "Save M3U File", "", "M3U Files (*.m3u);;All Files (*)", options=options)
        if fileName:
            with open(fileName, 'w', encoding='utf-8') as file:
                file.write(self.textEdit.toPlainText())

    def newM3U(self):
        self.textEdit.clear()
        self.categories = {}

    def addCategory(self):
        category, ok = QInputDialog.getText(self, 'Add Category', 'Enter category name:')
        if ok:
            if category not in self.categories:
                self.categories[category] = []
            self.updateTextEdit()

    def addChannel(self):
        name, ok1 = QInputDialog.getText(self, 'Add Channel', 'Enter channel name:')
        url, ok2 = QInputDialog.getText(self, 'Add Channel', 'Enter channel URL:')
        icon, ok3 = QInputDialog.getText(self, 'Add Channel', 'Enter icon URL (optional):')
        if ok1 and ok2:
            category, ok4 = QInputDialog.getText(self, 'Add Channel', 'Enter category (optional):')
            if not ok4 or category == '':
                category = "Uncategorized"
            if category not in self.categories:
                self.categories[category] = []
            if ok3 and icon:
                self.categories[category].append(f'#EXTINF:-1 tvg-logo="{icon}" group-title="{category}",{name}\n{url}')
            else:
                self.categories[category].append(f'#EXTINF:-1 group-title="{category}",{name}\n{url}')
            self.updateTextEdit()

    def showCategories(self):
        self.categoryList = QListWidget()
        self.categoryList.setSelectionMode(QListWidget.MultiSelection)
        for category in self.categories.keys():
            item = QListWidgetItem(category)
            self.categoryList.addItem(item)
        self.categoryList.itemClicked.connect(self.showChannels)

        deleteCategoryButton = QPushButton('Delete Category')
        renameCategoryButton = QPushButton('Rename Category')

        deleteCategoryButton.clicked.connect(self.deleteCategory)
        renameCategoryButton.clicked.connect(self.renameCategory)

        layout = QVBoxLayout()
        layout.addWidget(self.categoryList)
        layout.addWidget(deleteCategoryButton)
        layout.addWidget(renameCategoryButton)

        self.categoryWindow = QWidget()
        self.categoryWindow.setLayout(layout)
        self.categoryWindow.setWindowTitle('Categories')
        self.categoryWindow.show()

    def showChannels(self, item):
        category = item.text()
        self.channelList = QListWidget()
        self.channelList.setSelectionMode(QListWidget.MultiSelection)
        for channel in self.categories[category]:
            item = QListWidgetItem(channel)  # Display entire channel info in the list
            self.channelList.addItem(item)

        deleteButton = QPushButton('Delete Channel(s)')
        moveButton = QPushButton('Move Channel(s)')
        addButton = QPushButton('Add Channel(s) to Category')
        selectAllButton = QPushButton('Select All')  # Add a Select All button

        deleteButton.clicked.connect(lambda: self.deleteChannel(category))
        moveButton.clicked.connect(lambda: self.moveChannel(category))
        addButton.clicked.connect(lambda: self.addChannelToCategory(category))
        selectAllButton.clicked.connect(self.selectAllChannels)  # Connect to selectAllChannels method

        layout = QVBoxLayout()
        layout.addWidget(self.channelList)
        layout.addWidget(deleteButton)
        layout.addWidget(moveButton)
        layout.addWidget(addButton)
        layout.addWidget(selectAllButton)  # Add Select All button to layout

        self.channelWindow = QWidget()
        self.channelWindow.setLayout(layout)
        self.channelWindow.setWindowTitle(f'Channels in {category}')
        self.channelWindow.show()

        # Connect item selection in channelList to show corresponding channel info in textEdit
        self.channelList.itemSelectionChanged.connect(self.updateTextEditForChannelList)
        self.channelList.itemClicked.connect(self.handleChannelSelection)

    def handleChannelSelection(self, item):
        row = self.channelList.row(item)
        if row % 2 == 0:  # If the selected item is an EXTINF line
            self.channelList.setCurrentRow(row + 1, QItemSelectionModel.Select)
        else:  # If the selected item is a URL line
            self.channelList.setCurrentRow(row - 1, QItemSelectionModel.Select)

    def updateTextEditForChannelList(self):
        selected_items = self.channelList.selectedItems()
        content = ""
        for item in selected_items:
            content += item.text() + "\n"
        self.textEdit.setPlainText(content)

    def deleteChannel(self, category):
        selected_items = self.channelList.selectedItems()
        for item in selected_items:
            channel_info = item.text()
            if channel_info in self.categories[category]:
                self.categories[category].remove(channel_info)
        self.updateTextEdit()

    def moveChannel(self, category):
        selected_items = self.channelList.selectedItems()
        if selected_items:
            dialog = QInputDialog(self)
            dialog.setComboBoxItems(list(self.categories.keys()) + ["New Category"])
            dialog.setWindowTitle('Move Channel(s)')
            dialog.setLabelText('Select existing category or type a new category:')
            dialog.setComboBoxEditable(True)
            if dialog.exec() == QInputDialog.Accepted:
                new_category = dialog.textValue()
                if new_category:
                    for item in selected_items:
                        channel_info = item.text()
                        if channel_info in self.categories[category]:
                            self.categories[category].remove(channel_info)
                            new_channel_info = channel_info.replace(f'group-title="{category}"', f'group-title="{new_category}"')
                            if new_category not in self.categories:
                                self.categories[new_category] = []
                            self.categories[new_category].append(new_channel_info)
                    self.updateTextEdit()

    def addChannelToCategory(self, category):
        name, ok1 = QInputDialog.getText(self, 'Add Channel', 'Enter channel name:')
        url, ok2 = QInputDialog.getText(self, 'Add Channel', 'Enter channel URL:')
        icon, ok3 = QInputDialog.getText(self, 'Add Channel', 'Enter icon URL (optional):')
        if ok1 and ok2:
            if ok3 and icon:
                self.categories[category].append(f'#EXTINF:-1 tvg-logo="{icon}" group-title="{category}",{name}\n{url}')
            else:
                self.categories[category].append(f'#EXTINF:-1 group-title="{category}",{name}\n{url}')
            self.updateTextEdit()

    def deleteCategory(self):
        selected_items = self.categoryList.selectedItems()
        for item in selected_items:
            del self.categories[item.text()]
        self.updateTextEdit()

    def renameCategory(self):
        selected = self.categoryList.currentItem()
        if selected:
            old_category = selected.text()
            new_category, ok = QInputDialog.getText(self, 'Rename Category', 'Enter new category name:')
            if ok and new_category:
                self.categories[new_category] = self.categories.pop(old_category)
                for i, channels in enumerate(self.categories[new_category]):
                    if channels.startswith('#EXTINF:'):
                        self.categories[new_category][i] = channels.replace(f'group-title="{old_category}"', f'group-title="{new_category}"')
                self.updateTextEdit()

    def mergeM3Us(self):
        options = QFileDialog.Options()
        files, _ = QFileDialog.getOpenFileNames(self, "Select M3U Files to Merge", "", "M3U Files (*.m3u);;All Files (*)", options=options)
        if files:
            merged_categories = {}  # Temporary dictionary to hold merged categories
            for file in files:
                try:
                    with open(file, 'r', encoding='utf-8') as f:
                        content = f.read()
                except UnicodeDecodeError:
                    with open(file, 'r', encoding='latin-1') as f:
                        content = f.read()
                file_categories = parse_m3u(content)

                # Merge categories and channels
                for category, channels in file_categories.items():
                    if category not in merged_categories:
                        merged_categories[category] = []
                    merged_categories[category].extend(channels)

            # Append merged categories to existing categories
            for category, channels in merged_categories.items():
                if category not in self.categories:
                    self.categories[category] = []
                self.categories[category].extend(channels)

            # Update the text edit with the current categories
            self.updateTextEdit()

    def updateTextEdit(self):
        content = "#EXTM3U\n"  # Ensure #EXTM3U header is always present
        for category, channels in self.categories.items():
            for channel in channels:
                content += channel + "\n"
        self.textEdit.setPlainText(content)

    def selectAllChannels(self):
        self.channelList.selectAll()

def parse_m3u(content):
    categories = {}
    current_category = None
    for line in content.splitlines():
        if line.startswith('#EXTINF:'):
            match = re.search(r'group-title="(.*?)"', line)
            if match:
                current_category = match.group(1)
            else:
                current_category = "Uncategorized"
            if current_category not in categories:
                categories[current_category] = []
            categories[current_category].append(line)
        elif line.startswith('http'):
            if current_category:
                categories[current_category].append(line)
    return categories

def main():
    app = QApplication(sys.argv)
    editor = M3UEditor()
    editor.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()
